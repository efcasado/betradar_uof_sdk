# Architecture

The SDK connects a UOF feed to application callbacks. It owns transport consumption,
XML decoding, producer health, and recovery orchestration. The host application owns
business state and the effects of handling each message.

This guide explains the process boundaries, how the AMQP and Pulsar deployments differ,
and what happens during startup, recovery, and restarts. For installation, transport
options, and handler examples, see the [README](../README.md).

## Public Boundaries

Applications interact with a small set of modules:

| Module | Responsibility |
| --- | --- |
| `UOF.SDK` | Starts the SDK and exposes producer state and manual recovery |
| `UOF.SDK.MessageHandler` | Defines callbacks for feed content and producer-status changes |
| `UOF.SDK.Context` | Supplies producer identity, message type, routing key, and event URN to content callbacks |
| `UOF.SDK.ProducerMonitor.Store` | Defines persistence operations for consume sessions and recovery progress |

`uof_schemas` owns XML decoding and feed structs. `uof_api` owns the HTTP API client,
including producer descriptions and recovery requests. Broadway owns message demand,
processor concurrency, and acknowledgement dispatch. The transport adapters connect those
Broadway pipelines to RabbitMQ or Pulsar.

This division keeps API access, schema handling, and business logic out of the transport
implementation. Applications can use the API and schema packages independently of the SDK.

## The Supervision Tree

The SDK is a library supervisor, started explicitly by the host application:

```elixir
children = [
  MyApp.Repo,
  UOF.SDK
]

Supervisor.start_link(children, strategy: :one_for_one)
```

Infrastructure used by a custom store or handler should be available before the SDK starts.
The SDK itself uses `:rest_for_one` and starts children in dependency order:

```text
MyApp.Supervisor
└── UOF.SDK                           (:rest_for_one)
    ├── ProducerMonitor.Store        (if the store provides child_spec/1)
    ├── ProducerMonitor
    ├── AMQP.Connection or Pulsar.Client (selected transport)
    ├── SystemPipeline
    └── ContentPipeline
```

The transport client is one child selected by configuration. Each pipeline
is a Broadway supervision tree with one producer stage. System processing uses one
processor; content processing uses the configured processor concurrency.

A monitor restart also restarts the transport client and both pipelines. They must report
current consume sessions to the new monitor, and a Pulsar system consumer must report its
ownership again. A store-process restart also restarts the components that loaded its state.
A content-pipeline restart does not require restarting the system pipeline or monitor.
Failures handled inside a child, such as a channel reconnect, need not restart its siblings.

The SDK uses registered names for its monitor, pipelines, and transport client. The standard
configuration starts one SDK instance per VM; Pulsar scale-out uses multiple such instances.

## Direct AMQP

The AMQP transport owns one broker connection shared by the two pipelines:

```text
Betradar unifiedfeed exchange
             │
      one AMQP connection
             │
     ┌───────┴────────┐
 system channel   content channel
     │                │
 exclusive queue  exclusive queue
     │                │
 SystemPipeline   ContentPipeline
```

`UOF.SDK.AMQP.Connection` is the supervised coordinator implementing BroadwayRabbitMQ's
channel-checkout interface. It starts and monitors a `UOF.SDK.AMQP.Session` process that owns
connection establishment and the socket lifetime. The Session is monitored atomically at
startup, so even an immediate connection failure retains its exit reason.

Connection establishment runs asynchronously. While an attempt is pending, checkouts return
`:connecting` and Broadway retries with backoff. Once connected, each producer opens its own
channel so its consumer ownership remains local to Broadway. Checking a channel back in
requests an AMQP close; the protocol finishes teardown without force-killing the channel
and disrupting the shared connection.

The Session is outside the SDK supervision tree and monitors the coordinator. If the
coordinator stops during a handshake, the Session finishes the bounded open and closes its
result. Its registered name prevents a replacement attempt until cleanup completes. SDK
supervisor shutdown can therefore return before socket cleanup finishes. Connection cleanup
waits for graceful termination and uses a bounded wait before forcing disposal of the socket
process if it remains alive after the close call.

The system queue receives `alive` and `snapshot_complete`. The content queue receives event
messages and `alive` messages used to observe content freshness. Bindings also account for
`node_id`, which scopes recovery traffic to the intended feed session.

Each queue is exclusive and auto-deleted. Instances do not divide work through a shared
queue, so this mode scales processing through local Broadway concurrency. A fresh consume
session receives a new consumer tag, which the monitor uses to detect a possible feed gap.

Broadway owns reconnect backoff. A channel failure replaces that channel while the shared
connection remains available. A connection failure causes both consumers to obtain channels
on a replacement connection. Observing a previous transient connection failure also starts
the next attempt, while preserving the failure reason for reporting.

`UOF.SDK.AMQP.Client` adapts channel setup to BroadwayRabbitMQ's retry contract. Known transient
failures, including normal shutdown during a pending AMQP call, use backoff. Permanent
failures retain their cause, and unexpected exits and exceptions propagate after channel
cleanup. Permission rejection raises explicitly because Broadway otherwise retries it.
The SDK's setup-failure telemetry preserves the original reason and retry classification.

## Pulsar

The Pulsar deployment introduces durable buffering between the upstream feed connection and
SDK consumers:

```text
Betradar AMQP
     │
RabbitMQ source connector
     │
Pulsar topic
     ├── <subscription>-system  (Failover)
     │       └── active SDK instance → SystemPipeline → ProducerMonitor
     │
     └── <subscription>-content (Key-Shared)
             ├── SDK instance A → ContentPipeline → application handler
             ├── SDK instance B → ContentPipeline → application handler
             └── SDK instance C → ContentPipeline → application handler
```

Each instance starts one supervised `Pulsar.Client`, shared by its two subscriptions.
Instances with the same base subscription divide content delivery through Key-Shared.
The system subscription uses Failover to select the instance that coordinates producer
health and recovery. These are independent subscriptions to the same topic: the system
pipeline filters out event content before decoding it.

The connector publishes the original XML payload, uses the AMQP routing key as the Pulsar
message key, and includes its server-generated consumer tag in the
`__rabbitmq_consumer_tag` property. Key-Shared dispatch follows that message key. The SDK
reads the routing key and upstream consume-session identity from this metadata.

The supported topic has a single partition: either a non-partitioned topic or a partitioned
topic with exactly one partition. Failover ownership is assigned per partition, so this
layout gives the system subscription one active owner.

The monitor starts passive and waits for the broker's ownership report. The active owner
runs periodic health checks and issues recovery requests. Passive instances continue their
content processing. Demotion parks in-flight recoveries; promotion permits pending work to
resume. Manual recovery on a passive instance returns `{:error, :passive}`.

The connector is outside the SDK supervision tree. Restarting an SDK instance therefore
does not necessarily restart the upstream AMQP session. A durable subscription can retain
messages while that instance is offline; retention settings must preserve the backlog needed
for the deployment's restart behaviour.

## Message Processing

Content follows a short path:

```text
transport delivery
    → routing-key metadata
    → Broadway partition dispatch
    → XML decoding
    → application callback
    → content timestamp observation
    → transport acknowledgement
```

Within an instance, the content pipeline partitions messages by sport-event URN. Messages
for one event go to the same processor, while different events can be handled concurrently.
System messages use a separate pipeline so producer monitoring has its own processing lane.

The content callback runs synchronously in a Broadway processor. It receives the decoded
feed struct and a `UOF.SDK.Context`, and its return contract is `:ok`. Returning lets Broadway
complete the message. Applications that hand work to another process should make that
handoff durable before returning if it is the point at which they consider delivery complete.

Handler persistence and transport acknowledgement are separate operations. Applications
should make their effects idempotent because reconnects and recovery can replay messages.
Failures pass through the pipeline's failure callback, which logs context and emits
`[:uof_sdk, :message, :failed]` telemetry. The AMQP transport rejects failed messages without
requeue; Pulsar acknowledgement and redelivery follow the configured adapter behaviour.

Raw system messages are internal inputs. `alive` observations update producer health and
recovery progress, and `snapshot_complete` correlates recovery completion. Applications
receive lifecycle changes through `handle_producer_status/1` and can query state through
`UOF.SDK.producer/1` or `UOF.SDK.producers/0`.

## Startup and Recovery

Starting the supervisor establishes the runtime tree. Producer initialization loads persisted
session/progress records and fetches active producer descriptions through `uof_api`. Failure
to load descriptions fails startup: recovery needs the producer's API product and advertised
recovery window. Transport consumption and producer synchronization proceed after that
initialization; a successful `start_link/1` is not a feed-synchronization barrier.

Recovery is a coordinated sequence:

1. An initial synchronization need, session change, health observation, or manual request
   creates recovery intent for a producer.
2. The monitor persists that the producer requires recovery before issuing HTTP.
3. Once ownership, connection gates, and cooldown permit it, the SDK requests recovery with
   a request ID. A stored checkpoint selects incremental recovery; without one, recovery
   requests a full snapshot.
4. Replayed content arrives through the normal feed and application callbacks.
5. A matching `snapshot_complete` ends the recovery job and updates producer state.

Failed HTTP requests are retried after the recovery interval. A stall timer reissues a request
whose completion has not arrived, retaining its original recovery timestamp. Superseded retry
messages are ignored using recovery-job generations.

`snapshot_complete` marks the feed's completion of a recovery replay. Content callbacks run
on their own processors, so this system observation is separate from application processing
completion. Content timestamps supply the monitor's processing-delay observations.

The lifecycle states describe the monitor's view:

| State | Meaning |
| --- | --- |
| `:down` | Unsynchronized, with no pending recovery job |
| `:recovering` | Recovery is pending, being requested, or awaiting completion |
| `:up` | The monitor considers the producer synchronized |
| `:delayed` | Processing observations exceed the configured delay threshold |
| `:resuming` | Restored progress is eligible for resume, pending continuity and freshness checks |

A pending or in-flight recovery job is projected as `:recovering` when state is reported.
Internal request functions, timers, and job correlation stay out of the public producer view.

## Persistence and Restart Resume

The store holds two kinds of durable records:

| Record | Contents |
| --- | --- |
| Session | Committed system/content consume tokens and a generation |
| Producer progress | Recovery checkpoint and the generation in which the producer synchronized |

Changing a consume session atomically advances its generation. Every producer synchronized
in an older generation then requires recovery, without a transaction updating all producer
records. Persisting recovery intent before external I/O also ensures a restart can rediscover
unfinished work.

Subscribed system heartbeats advance checkpoints after synchronization. Incremental recovery
subtracts the configured overlap from the checkpoint and clamps the result to the producer's
advertised recovery window. A checkpoint selects a replay start time; it is separate from an
application transaction or broker acknowledgement.

At startup, a producer with a checkpoint and a matching synchronization generation enters
`:resuming`. Persisted tokens are comparison baselines. The current pipelines must report their
sessions again, and the monitor uses heartbeat and content-freshness observations to decide
when to leave that state. Session-readiness deadlines prevent an incomplete restart from
waiting indefinitely.

Direct AMQP reconnects create new consume sessions and require recovery. Pulsar can resume
retained backlog when its upstream connector session remains unchanged. An upstream session
change or recovery-required heartbeat still enters the normal recovery path.

The default ETS store survives monitor and pipeline restarts while its owning process remains
alive. It loses state when that process or VM stops. A persistent store supports progress
across VM restarts and must honor the behaviour's atomic mutation contracts and single-writer
ownership. The [README's store guide](../README.md#monitor-state-persistence) describes those
callbacks and how to supervise their backing infrastructure.

## Implementation Notes for Contributors

`ProducerMonitor` is the coordination GenServer. It routes observations, applies ownership
and connection gates, persists transitions, and publishes producer-status callbacks.
`ProducerMonitor.Producer` owns per-producer health transitions, recovery jobs, cooldowns,
HTTP attempts, and timers. These functions execute in the monitor process; status callbacks
also run there and should complete promptly.

`ProducerMonitor.Connections` tracks consume-session tokens independently of producer state.
`Transport` derives the two producer specifications and the shared transport child from one
configuration. `MessageMetadata` normalizes transport metadata, and `RoutingKey` supplies
routing and event identity. The pipelines own decoding and dispatch rather than recovery
policy.

Useful entry points are the [SDK supervisor](../lib/uof/sdk.ex),
[transport wiring](../lib/uof/sdk/transport.ex),
[producer monitor](../lib/uof/sdk/producer_monitor.ex), and
[store behaviour](../lib/uof/sdk/producer_monitor/store.ex).

## Design Invariants

1. The monitor starts before transport consumers that report sessions or ownership to it.
2. Direct AMQP pipelines share one connection and own separate channels and queues.
3. Only the active Pulsar system-subscription owner issues recovery requests.
4. Recovery intent is persisted before its HTTP request is issued.
5. Session generations invalidate older synchronization records without a multi-producer write.
6. Content callbacks own application effects; the SDK owns feed delivery and recovery orchestration.

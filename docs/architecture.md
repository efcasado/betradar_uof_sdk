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

> [!WARNING]
> If batching is enabled for the RabbitMQ source connector's Pulsar producer, use
> key-based batching (`KEY_BASED`). Alternatively, disable producer batching.
> Default batching can combine different routing keys in one batch, which Pulsar
> routes using the first message's key. This breaks the Key-Shared distribution
> semantics required by the content subscription. Configure this on the connector's
> producer; SDK consumer configuration cannot repair mixed-key batches. See
> [Pulsar's Key-Shared producer requirements](https://pulsar.apache.org/docs/4.2.x/concepts-messaging/).

The supported topic has a single partition: either a non-partitioned topic or a partitioned
topic with exactly one partition. Failover ownership is assigned per partition, so this
layout gives the system subscription one active owner.

Ownership reports are coordination signals, not fencing tokens; brief overlap during
failover can issue duplicate recovery requests. Each instance uses its own stored progress
when it becomes active.

The monitor starts passive and waits for the broker's ownership report. The active owner
runs periodic health checks and issues recovery requests. Passive instances continue their
content processing. Demotion parks in-flight recoveries; promotion permits pending work to
resume. Manual recovery on a passive instance returns `{:error, :passive}`.

The connector is outside the SDK supervision tree. Restarting an SDK instance therefore
does not necessarily restart the upstream AMQP session. A durable subscription can retain
messages while that instance is offline; retention settings must preserve the backlog needed
for the deployment's restart behaviour. Disable message TTL or set it above the worst-case
downtime, and use a `producer_exception` backlog quota policy to prevent eviction.

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

### Ordering and Scale-Out

Within an instance, the content pipeline partitions messages by sport-event URN. Messages
for one event go to the same processor, while different events can be handled concurrently.
System messages use a separate pipeline so producer monitoring has its own processing lane.
There is no processing-completion ordering between the system and content pipelines.

Pulsar dispatch and local Broadway partitioning use different keys. The connector supplies
the full AMQP routing key as the Pulsar message key, while Broadway extracts the event URN
for local dispatch. Different routing keys for the same event can therefore reach different
SDK instances. Local event partitioning does not establish event-wide ordering across those
instances. Applications that require that guarantee must account for the upstream keying
and their distributed processing design.

### Handler Execution and Delivery

The content callback runs synchronously in a Broadway processor. It receives the decoded
feed struct and a `UOF.SDK.Context`, and its return contract is `:ok`. Returning lets Broadway
complete the message. Applications that hand work to another process should make that
handoff durable before returning if it is the point at which they consider delivery complete.

The pipeline does not interpret callback return values as failure signals: returning
`{:error, reason}` is not a request to reject or retry a message. Decoding errors and raised
processing failures enter Broadway's failure path. Applications own retries of handler
side effects and must account for duplicate or stale deliveries when doing so.

`handle_producer_status/1` runs synchronously in `ProducerMonitor`, rather than a content
processor. A slow status callback blocks monitor observations, recovery coordination, and
state queries; a callback exception fails that monitor process. Keep status callbacks short.
Store calls and recovery HTTP requests also execute in the monitor, so their latency affects
when it can process the next observation or timer message.

Handler persistence and transport acknowledgement are separate operations. Applications
should make their effects idempotent because reconnects and recovery can replay messages.
Failures pass through the pipeline's failure callback, which logs context and emits
`[:uof_sdk, :message, :failed]` telemetry. The AMQP transport rejects failed messages without
requeue; Pulsar acknowledgement and redelivery follow the configured adapter behaviour.
A message failure emits diagnostics but does not directly request producer recovery.

Content delivery is not gated on producer health. Live and replayed content can reach handlers
while a producer is `:down`, `:recovering`, or `:delayed`. The SDK reports synchronization and
health; applications decide how those reports affect their business operations. A producer
becoming `:up` is not a barrier proving that all application side effects have completed.

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

## Health and Operational Signals

A running supervisor, an established transport connection, and a synchronized producer are
separate states. Use producer status to understand feed synchronization and processing lag;
use transport and message diagnostics to investigate why progress has stopped.

### Time and Recovery Controls

Public duration settings are in seconds and converted to milliseconds internally. Feed
timestamps and stored checkpoints are epoch milliseconds. Heartbeat inactivity uses local
observation time; content lag compares local wall-clock time with processed feed timestamps.
Clock skew can therefore affect lag observations. Recovery cooldown uses monotonic time.

| Setting | What it measures or controls | Effect |
| --- | --- | --- |
| `inactivity_seconds` | Elapsed time since a system heartbeat or matching recovery completion established liveness | Requests recovery on an observed heartbeat timeout; also bounds session-readiness waiting |
| `max_processing_delay_seconds` | Age of the latest observed content timestamp, including content-session heartbeats | Moves synchronized producers between `:up` and `:delayed`; freshness also gates restart resume |
| `min_interval_between_recoveries` | Per-producer recovery-request cooldown and delay after a failed request | Defers requests or retries while recovery remains pending |
| `max_recovery_time` | Wait for matching `snapshot_complete` after a successful recovery request | Reissues a stalled recovery while retaining its original replay timestamp |
| `recovery_overlap_seconds` | Look-back subtracted from a stored checkpoint | Replays overlapping data during incremental recovery, bounded by the producer's recovery window |

These controls have distinct purposes. Content lag alone is not a heartbeat failure, and
recovery overlap is not a timeout. Health checks and timer messages are handled by the
monitor, subject to control-plane ownership and its current recovery state; durations are
not hard real-time deadlines. See the [configuration reference](../README.md#configuration)
for defaults and the [recovery guide](../README.md#producer-health-and-recovery) for usage.

### Observability

| Signal | What it tells the application |
| --- | --- |
| `UOF.SDK.producers/0` and `UOF.SDK.producer/1` | Current per-producer lifecycle state and observations |
| `handle_producer_status/1` | Reported producer-state transitions, delivered synchronously by the monitor |
| `[:uof_sdk, :message, :failed]` | A decoding or processing failure, with routing context and failure reason |
| `[:uof_sdk, :recovery, :initiated]` | A successful recovery request, with producer ID, request ID, and replay start timestamp; not recovery completion |
| `[:uof_sdk, :amqp, :setup_failure]` | A returned AMQP setup failure, preserving the operation, original reason, and retry classification |
| `[:broadway_rabbitmq, :amqp, :open_connection, :start / :stop / :exception]` | Spans around shared AMQP connection establishment |

Correlate recovery logs and initiation events by producer and request ID, then observe
producer status for lifecycle progress. Message-failure logs include routing context and
a truncated payload. AMQP diagnostics distinguish expected retries from permanent failures;
unexpected exceptions and exits also appear through process failure reports. A quiet error
log alone does not establish readiness or prove that handlers have completed their work.

## ProducerMonitor.Store

`UOF.SDK.ProducerMonitor.Store` is the persistence boundary for the monitor's recovery
and restart decisions. The monitor owns the policy and calls the store; applications
choose the storage implementation. The store holds consume-session identity and producer
progress. Feed messages, application business state, broker acknowledgements, and runtime
recovery jobs remain outside this contract.

The default `Store.ETS` implementation keeps records in a table owned by its supervised
process. Records survive monitor and pipeline restarts while that process remains alive,
but disappear when the store process or VM stops. Implement a persistent store when progress
must survive those boundaries. A persistent backend can serve several SDK instances, but
each monitor must have its own logical store with exactly one writer.

### Stored Records

The behaviour defines two structs:

| Record | Fields | Meaning |
| --- | --- | --- |
| `Store.Session` | `tokens`, `generation` | Committed system/content consume tokens and their monotonically increasing generation |
| `Store.ProducerProgress` | `checkpoint`, `synchronized_generation` | One producer's recovery timestamp in milliseconds and the generation in which it synchronized |

Changing a consume session atomically advances its generation. Every producer synchronized
in an older generation then requires recovery, without a transaction rewriting all producer
records. A producer is eligible for restart resume only when it has a checkpoint and its
`synchronized_generation` matches the current session generation. Eligibility still requires
runtime continuity and freshness checks before the monitor considers the producer up.

A genuinely empty store returns `%Store.Session{}` and `%{}` from its load callbacks.
Mutations for a previously unseen producer start from `%Store.ProducerProgress{}`. These
initial values represent missing records, not a fallback for an unavailable backend.

### Callback Lifecycle

Callbacks execute synchronously in the monitor process. Load callbacks return records;
mutation callbacks return the resulting record directly, which the monitor adopts as its
current state. They do not return `:ok` or `{:ok, record}`.

| Callback | When the monitor uses it | Required result and effect |
| --- | --- | --- |
| `load_session/0` | Monitor initialization | Return the committed `Store.Session` |
| `load_producer_progress/0` | Monitor initialization | Return a map of producer IDs to `Store.ProducerProgress` |
| `commit_session_change/1` | Current consume tokens change | Atomically store the token map and increment the generation; return the new session |
| `advance_checkpoint/2` | An eligible subscribed system heartbeat advances progress | Advance that producer's checkpoint monotonically, preserve its synchronization generation, and return its progress |
| `require_recovery/1` | A producer must no longer resume as synchronized | Clear its synchronization generation, preserve its checkpoint, and return its progress |
| `mark_synchronized/2` | A producer is considered synchronized in the current session | Store the supplied generation, preserve its checkpoint, and return its progress |

The monitor avoids writes when its current records already represent the required state.
Content messages and content-session heartbeats do not write checkpoints. A checkpoint
selects a recovery replay start time; it is separate from an application transaction or
broker acknowledgement.

For example, suppose the session generation is 7 and producer 1 has a checkpoint and
`synchronized_generation: 7`. A changed consume token commits generation 8. Producer 1's
unchanged progress is now ineligible to resume. Recovery preparation records that recovery
is required before issuing HTTP; successful synchronization records generation 8. If the
monitor crashes after the session commit or recovery preparation, the persisted state still
prevents it from resuming as though no gap occurred.

### Implementing and Configuring a Store

Implement the six callbacks in the [store behaviour](../lib/uof/sdk/producer_monitor/store.ex)
and configure the module:

```elixir
config :uof_sdk, monitor_store: MyApp.ProducerMonitorStore
```

Each mutation must atomically update its session record or producer record and preserve
unrelated fields. For a persistent implementation, complete the durable write before
returning. A successful return must not mean that a write has merely been queued elsewhere.
Maintain checkpoint monotonicity even when an older timestamp is supplied.

The callback API has no error-tuple contract. Let backend failures raise or exit so startup
or supervision can handle them visibly. Returning empty records on a failed load or claiming
a failed write succeeded would give the monitor an incorrect recovery baseline. Because
calls run in the monitor, configure backend timeouts appropriate to its availability needs.

Each logical store has exactly one writer: its `ProducerMonitor`. Concurrent writes from
another monitor, SDK instance, or administration tool are unsupported. With a shared
database, isolate each monitor's session and producer records in a stable namespace that
survives that instance's restart. The callback API takes no store-instance argument; the
configured module is responsible for selecting its backend and namespace. The store does
not elect the active Pulsar consumer or coordinate ownership between SDK instances.

If the backend is already supervised by the host application, start it before the SDK and
omit the optional store `child_spec/1`:

```elixir
children = [
  MyApp.Repo,
  UOF.SDK
]
```

If the store owns a process, implement `child_spec/1` (for example through `use GenServer`
and `start_link/1`). The SDK starts that store module as its first child, before loading
monitor state. The SDK supplies the module as the child specification, so its default child
argument is `[]`; backend configuration belongs to the store implementation. A store-process
restart also restarts the monitor and downstream consumers through `:rest_for_one`.

Use the [ETS implementation](../lib/uof/sdk/producer_monitor/store/ets.ex) as a small reference
for callback semantics, and the [store tests](../test/uof/sdk/producer_monitor/store/ets_test.exs)
as examples of the contract. For a persistent implementation, also verify records survive
backend/client restarts, failed operations remain visible, and separate monitor namespaces
do not overwrite each other. Cover empty initialization, monotonic checkpoints, preservation
of unrelated fields, generation changes, and recovery invalidation.

### Restart Resume

At startup, a producer with a checkpoint and a matching synchronization generation enters
`:resuming`. Persisted tokens are comparison baselines. The current pipelines must report
their sessions again, and the monitor uses heartbeat and content-freshness observations to
decide when to leave that state. Session-readiness deadlines prevent an incomplete restart
from waiting indefinitely.

Direct AMQP reconnects create new consume sessions and require recovery. Pulsar can resume
retained backlog when its upstream connector session remains unchanged. An upstream session
change or recovery-required heartbeat still enters the normal recovery path. Persistence
alone does not prove delivery continuity or replace broker backlog retention.

Incremental recovery subtracts the configured overlap from the checkpoint and clamps the
result to the producer's advertised recovery window. Handlers must tolerate replayed
messages. The [README's persistence guide](../README.md#implementing-a-producermonitor-store-backend) describes
the configuration and operational requirements alongside the transport setup.

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

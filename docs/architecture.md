# Architecture

How the SDK is built: its components, how it consumes the feed, and the contracts between
them. The [deployment guide](deployment.md) covers what to set up, and the
[recovery guide](recovery.md) how producers are kept synchronized.

- [Overview](#overview): public boundaries and the supervision tree
- [Feed topology](#feed-topology): the topic, subscriptions, ownership, and feed continuity
- [Message processing](#message-processing): ordering, delivery, and failures
- [Producer monitor](#producer-monitor): the control plane
- [Persistence](#persistence): the monitor snapshot and store contract
- [Observability](#observability): health and telemetry signals
- [Internals](#internals): modules and where they run
- [Design invariants](#design-invariants): the rules changes must preserve

## Overview

### Public Boundaries

Applications interact with a small set of modules:

| Module | Responsibility |
| --- | --- |
| `UOF.SDK` | Starts the SDK and exposes producer state and manual recovery |
| `UOF.SDK.MessageHandler` | Defines callbacks for feed content and producer-status changes |
| `UOF.SDK.ProducerStatus` | Reports a producer's lifecycle state |
| `UOF.SDK.Context` | Supplies producer identity, message type, routing key, and event URN to content callbacks |
| `UOF.SDK.ProducerMonitor.Store` | Persists the monitor's restart snapshot |

`uof_schemas` owns XML decoding and feed structs. `uof_api` owns the HTTP API client,
including producer descriptions and recovery requests. Broadway owns message demand,
processor concurrency, and acknowledgement dispatch. `off_broadway_pulsar` connects those
Broadway pipelines to Pulsar.

This division keeps API access, schema handling, and business logic out of the consumer
implementation. Applications can use the API and schema packages independently of the SDK.

### The Supervision Tree

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
    ├── Pulsar.Client
    ├── SystemPipeline
    └── ContentPipeline
```

The Pulsar client is shared by both pipelines. Each pipeline is a Broadway supervision tree
with one producer stage. System processing uses one processor; content processing uses the
configured processor concurrency.

A monitor restart also restarts the Pulsar client and both pipelines. The system pipeline
must report the current consume session to the new monitor, and its consumer must report
its ownership again. A store-process restart also restarts the components that loaded its
state. A content-pipeline restart does not require restarting the system pipeline or monitor.
Failures handled inside a child, such as a broker reconnect, need not restart its siblings.

The SDK uses registered names for its monitor, pipelines, and Pulsar client. The standard
configuration starts one SDK instance per VM; scale-out uses multiple such instances.

## Feed Topology

Betradar delivers UOF over AMQP. A RabbitMQ source connector consumes it into a Pulsar topic,
which buffers the feed durably between the upstream connection and SDK consumers:

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

### Why a Pulsar Topic

The official SDKs consume Betradar's AMQP feed directly, through exclusive, auto-deleted
queues. Putting a durable topic between Betradar and the SDK changes what a restart, a
failover, or a slow handler costs:

| | Direct AMQP (official SDKs) | Pulsar topic (this SDK) |
| --- | --- | --- |
| SDK restart or deploy | The queue is deleted on disconnect, so queued messages are lost and every producer recovers | The connector keeps the Betradar session and the topic keeps the backlog: an unchanged session [resumes](recovery.md#restarts) without recovery |
| Failover between instances | Not applicable: each process holds its own session | With a shared [monitor store](deployment.md#monitor-store), the promoted instance resumes without recovery |
| Scaling | Each process gets its own queues and the whole feed; work scales only within a process | Key-Shared divides content across instances by sport event, keeping per-event ordering |
| Slow processing | Messages back up in Betradar's queue, and the official SDKs report the producer down or delayed while processing lags | Content waits in the content subscription; [feed continuity](#feed-continuity), and so recovery, does not depend on processing |
| Recovery requests | After every reconnect or restart, against Betradar's [rate limits](recovery.md#rate-limits) | Only after an upstream gap or a producer-side interruption |
| Operations | Nothing extra to run | Pulsar and the RabbitMQ source connector, set up as the [deployment guide](deployment.md) describes |

The topic also gives failed messages somewhere to go: they are dead-lettered for inspection
rather than lost.

### Subscriptions

Each instance starts one supervised `Pulsar.Client`, shared by its two subscriptions.
Instances with the same base subscription divide content delivery through Key-Shared. The
system subscription uses Failover to select the instance that coordinates producer health and
recovery. These are independent subscriptions to the same topic: the system pipeline
acknowledges event content without decoding it, and the content pipeline does the same with
`alive` and `snapshot_complete`.

### Connector Contract

The connector publishes the original XML payload and keeps routing metadata separate from the
key used for distributed dispatch:

| Field | Purpose |
| --- | --- |
| Pulsar message key | Sport-event URN for event messages; `system` for `alive` and `snapshot_complete` |
| `__rabbitmq_routing_key` property | Original AMQP routing key, used by the SDK for message type, event identity, and node filtering |
| `__rabbitmq_consumer_tag` property | The upstream session's consumer tag, used to detect possible gaps and to resume after an SDK restart |

The message key is the ordering scope. Keying event messages by event URN sends every message
type for one event, such as `odds_change` and `bet_stop`, to the same instance; keying by the
full routing key would split them. A batch is routed by its first message's key, so batches
must not mix keys. The [deployment guide](deployment.md#source-connector) shows the connector
configuration that produces this contract.

### Ownership and Failover

The supported topic has a single partition. Failover ownership is assigned per partition, so
the system subscription has one active owner.

The monitor starts passive and waits for the broker's ownership report. The active owner runs
periodic health checks, issues recovery requests, and writes the snapshot. Passive instances
continue their content processing. Demotion parks in-flight recoveries; promotion reloads the
snapshot, restores producers from it as a restart does, and permits pending work to resume.
The monitor also watches the active system consumer process and demotes when it exits, because
an exit may arrive without a final passive ownership report. A replacement consumer must
report ownership before the monitor resumes control-plane work.
Ownership reports are coordination signals, not fencing tokens: brief overlap during failover
can issue duplicate recovery requests, which are harmless beyond quota.

### Feed Continuity

With one partition and a Failover subscription, the active system consumer receives every
message on the topic. A delivery gap can only start upstream of the topic, when the
connector's consume session changes, and every message carries that session's consumer tag.
Content not yet processed stays in the content subscription's backlog. The system pipeline
alone therefore decides readiness, session changes, checkpoints, and recovery; the content
pipeline does not report to the monitor.

Producer status therefore describes feed continuity on the topic, not application progress:

- `:up` means the topic carries a continuous feed for the producer. Content may still be
  waiting in the content subscription.
- Content lag is not tracked. With Key-Shared, one monitor would see only its own share of
  content anyway. Use handler timestamps or the content subscription's backlog statistics to
  measure processing lag across instances.
- The checkpoint advances with subscribed system heartbeats while `:up`, regardless of how far
  content processing has got. An incremental recovery starts 60 seconds before that
  checkpoint to cover short publication reorderings between system and event keys in the
  connector's key-based batches. The overlap is a mitigation, not a bound on how old an
  unpublished event can be; see [recovery windows](recovery.md#how-a-recovery-runs).

This relies on the content cursor never losing a message the system subscription has seen.
Once the content subscription exists, its backlog holds everything it has not acknowledged.
Before it exists, nothing protects what the system subscription acknowledges, so deployments
create the subscriptions in advance or retain acknowledged messages; see
[subscriptions](deployment.md#subscriptions).

This differs from the official SDKs, which tie the checkpoint to content progress. The Java
SDK ([`e677b92`](https://github.com/sportradar/UnifiedOddsSdkJava/tree/e677b9222bf59b770384379f62f789778ddc8053))
advances it from system-session alives only while the producer is not flagged down, and flags
it down when processing lags (`ProcessingQueueDelayViolation`). The .NET SDK
([`a6045c1`](https://github.com/sportradar/UnifiedOddsSdkNetCore/tree/a6045c1d255c0b1510a0e70f0696b7e140645303))
uses the oldest alive seen on the user sessions. The
[recovery documentation](https://docs.sportradar.com/uof/sdk/features/recovery) recommends
storing the last processed message timestamp, and states that slow processing marks a producer
down without issuing recovery. All three consume AMQP directly, where unprocessed content is
lost with its queue. Here the checkpoint tracks feed continuity on the topic rather than
application processing; the recovery overlap allows for short cross-key publication delays.

### Node Isolation

The source connector binds `#`, so a topic can contain recovery traffic for several
subscriptions using the same access token. Both pipelines inspect the original routing key and
acknowledge messages addressed to another node without decoding them, invoking a handler, or
updating the monitor. Unaddressed messages (`-` in the node position) are accepted by every
node. A `nil` or non-positive `node_id` disables this filtering. `node_id` therefore
identifies a subscription, not an instance; the [deployment guide](deployment.md#sdk-configuration)
describes how to assign it.

## Message Processing

Content follows a short path:

```text
Pulsar delivery
    → routing-key property
    → Broadway partition dispatch
    → message-type and node filtering
    → XML decoding
    → application callback
    → Pulsar acknowledgement
```

### Ordering and Scale-Out

Within an instance, the content pipeline partitions messages by sport-event URN. Messages for
one event go to the same processor, while different events can be handled concurrently. System
messages use a separate pipeline so producer monitoring has its own processing lane. There is
no processing-completion ordering between the system and content pipelines.

With the [connector contract](#connector-contract), Key-Shared dispatch and local Broadway
partitioning use the same event identity, so all message types for one event reach the same
instance and the same local processor.

This preserves the per-event processing lane, not exactly-once delivery or a total order
across events. Reconnects, redelivery, and recovery can replay messages. Applications must
still handle duplicates and stale updates, and preserve ordering themselves if they offload
work from the synchronous callback.

The callbacks themselves are listed in the [README](../README.md#handler-reference).

### Handler Execution and Delivery

The content callback runs synchronously in a Broadway processor. It receives the decoded feed
struct and a `UOF.SDK.Context`, and its return contract is `:ok`. Returning lets Broadway
complete the message. Applications that hand work to another process should make that handoff
durable before returning if it is the point at which they consider delivery complete.

The pipeline does not interpret callback return values as failure signals: returning
`{:error, reason}` is not a request to reject or retry a message. Decoding errors and raised
processing failures enter Broadway's failure path. Applications own retries of handler side
effects and must account for duplicate or stale deliveries when doing so.

Handler persistence and Pulsar acknowledgement are separate operations. Applications should
make their effects idempotent because reconnects and recovery can replay messages. Failures
pass through the pipeline's failure callback, which logs context and emits
`[:uof_sdk, :message, :failed]` telemetry. The pipeline nacks a failed message and, about a
second later, Pulsar diverts it to the `<topic>-<subscription>-DLQ` topic instead of
processing it again, so it remains available for inspection; batch-index acknowledgement keeps
the failure from redelivering the other messages of its batch. Override `:consumer_opts`
(`:redelivery_interval`, `:dead_letter_policy`, `:batch_index_ack_enabled`) to change this.
A message failure emits diagnostics but does not request producer recovery.

Content delivery is not gated on producer health. Live and replayed content can reach handlers
while a producer is `:down` or `:recovering`, and content can still be waiting in the backlog
while it is `:up`. The SDK reports synchronization; applications decide how it affects their
business operations.

## Producer Monitor

`ProducerMonitor` is the control plane: a GenServer that owns producer status, recovery
orchestration, and the persisted snapshot. Its inputs all come from the system pipeline
(`alive`, `snapshot_complete`, and the upstream consumer tag), from broker ownership reports,
and from manual requests. The [recovery guide](recovery.md) describes its behaviour.

At startup it loads its snapshot and fetches the active producer descriptions through
`uof_api`; recovery needs each producer's API product and recovery window. Fetched
descriptions are stored in the snapshot. If the API fails, the monitor starts from the stored
descriptions, or else from the configured `:producers`, logs a warning, and keeps retrying in
the background. Startup fails only when none of the three has descriptions. An `alive` from an
unknown producer also reloads the descriptions, at most once per minute. A successful
`start_link/1` is not a feed-synchronization barrier.

`handle_producer_status/1` runs synchronously in the monitor. A slow status callback blocks
observations, recovery coordination, and state queries; a callback exception fails the
monitor. Store calls also execute in the monitor, so their latency affects when it can process
the next observation. Recovery requests and description reloads run in tasks linked to the
monitor: a slow API does not block it, and an exception from the request still fails it. The
startup description fetch is the exception; it runs before the monitor takes observations.

## Persistence

`UOF.SDK.ProducerMonitor.Store` persists the monitor's restart state. The monitor owns that
state and its rules; a store keeps only the latest snapshot, an opaque binary encoded by
`UOF.SDK.ProducerMonitor.Snapshot`. Feed messages, application state, broker
acknowledgements, and in-flight recovery jobs are not part of it.

The default `Store.ETS` keeps snapshots in a table owned by its supervised process. They
survive monitor and pipeline restarts, but not a store-process or VM restart. Implement a
persistent store when progress must survive those.

### Snapshot Contents

| Field | Meaning |
| --- | --- |
| `consumer_tag` | Committed consumer tag of the connector's upstream session |
| `producers` | Per producer: recovery checkpoint (epoch milliseconds) and the recovery it still owes: none when synchronized with that session, `:incremental` from the checkpoint, or `:full` |
| `descriptions` | The last producer descriptions loaded from the UOF API |

A producer may resume at startup only when it is synchronized and has a checkpoint. A consumer
tag change saves the new tag with every producer unsynchronized, in one write. Recovery
preparation saves the producer as unsynchronized before its HTTP request, so a crash at any
point cannot resume from stale state. A pending full recovery stays full across a restart or
session change until its matching completion. Checkpoints only move forward.

The monitor saves only when the snapshot changes: on a session change, a checkpoint advance
from a subscribed system heartbeat while `:up`, a producer's synchronization change, and newly
loaded descriptions.

`load/1` returns `nil` only when nothing has been saved, and the monitor then starts every
producer `:down`. A stored binary that cannot be decoded fails startup: discarding it would
lose the checkpoints that bound incremental recovery.

### Implementing a Store

A store implements `load(opts)` and `save(snapshot, opts)`, and optionally `child_spec/1`.
Configure it as a module or `{module, opts}`; `opts` is passed to every callback:

```elixir
config :uof_sdk, monitor_store: {MyApp.MonitorStore, repo: MyApp.Repo}
```

`save/2` must replace the snapshot atomically and return `:ok` only once the write is durable.
Let backend failures raise or exit; never return `nil` for a failed read or `:ok` for a failed
write. Callbacks run in the monitor, so a slow backend delays health checks and
`UOF.SDK.producers/0`; set backend timeouts accordingly. A failed write crashes the monitor,
and a backend that stays unavailable eventually stops `UOF.SDK` through supervisor restart
limits.

A store keeps one snapshot. All SDK instances of a deployment use the same store and share
it; only the instance that owns the system subscription writes it, and a passive instance's
copy is stale and changes only in memory until promotion reloads the stored one. Two
deployments sharing a database need separate storage, for example a table each through the
store's options.

For example, a PostgreSQL store using Ecto:

```elixir
# migration
create table(:uof_sdk_monitor_snapshot, primary_key: false) do
  add :name, :string, primary_key: true
  add :snapshot, :binary, null: false
end
```

```elixir
defmodule MyApp.MonitorStore do
  @behaviour UOF.SDK.ProducerMonitor.Store

  import Ecto.Query

  @table "uof_sdk_monitor_snapshot"
  # The store keeps one snapshot, in a single row.
  @name "snapshot"

  @impl true
  def load(opts) do
    repo = Keyword.fetch!(opts, :repo)
    repo.one(from(s in @table, where: s.name == @name, select: s.snapshot), timeout: 2_000)
  end

  @impl true
  def save(snapshot, opts) do
    repo = Keyword.fetch!(opts, :repo)

    {1, _} =
      repo.insert_all(@table, [%{name: @name, snapshot: snapshot}],
        on_conflict: {:replace, [:snapshot]},
        conflict_target: :name,
        timeout: 2_000
      )

    :ok
  end
end
```

Start the repo before the SDK. A store that owns a process implements `child_spec/1`; the SDK
starts `{module, opts}` as its first child, and a store-process restart also restarts the
monitor and consumers through `:rest_for_one`.

Test a persistent store for an empty load, replacement, and visible failures.
The [ETS store tests](../test/uof/sdk/producer_monitor/store/ets_test.exs) show the contract.

## Observability

A running supervisor, an established Pulsar connection, and a synchronized producer are
separate states. Use producer status to understand feed synchronization; measure processing
lag from handler timestamps or the content subscription's backlog, and use Pulsar and message
diagnostics to investigate why progress has stopped.

| Signal | What it tells the application |
| --- | --- |
| `UOF.SDK.producers/0` and `UOF.SDK.producer/1` | Current per-producer lifecycle state |
| `handle_producer_status/1` | Reported producer-state transitions, delivered synchronously by the monitor |
| `[:uof_sdk, :message, :failed]` | A decoding or processing failure, with routing context and failure reason |
| `[:uof_sdk, :recovery, :initiated]` | A successful recovery request, with producer ID, request ID, and replay start timestamp; not recovery completion |
| `[:uof_sdk, :recovery, :completed]` | A matching `snapshot_complete`, with `duration_ms` since the completed request was issued |
| `[:uof_sdk, :recovery, :failed]` | A rejected or failed recovery request, with its reason and `retry_in_ms` |
| `[:uof_sdk, :recovery, :stalled]` | A request with no `snapshot_complete` within `max_recovery_time`, emitted before it is reissued |
| `[:uof_sdk, :recovery, :interrupted]` | An unsubscribed `alive` or heartbeat gap while a request is in flight, with `reason` (`:unsubscribed` or `:alive_timeout`); the recovery repeats once it completes |
| `[:uof_sdk, :producer, :status]` | A reported producer status transition, emitted whether or not a handler is configured |
| `[:uof_sdk, :producer, :alive]` | A system `alive` was read, with `delay_ms`: its observation time minus its generation timestamp, including clock skew with Betradar. Also reported as `ProducerStatus.alive_delay` |
| `[:uof_sdk, :producers, :loaded]` | Producer descriptions loaded, with `count` and `source` (`:api`, `:snapshot`, or `:config`); a fallback source means the API was unavailable |
| `[:uof_sdk, :message, :other_node]` | A message addressed to another `node_id`, acknowledged without processing; expected when subscriptions share an access token |
| `[:pulsar, :consumer, ...]` | Consumer events from the `pulsar` client, such as nacks, redelivery requests, and dead-letter diversions |

The [recovery guide](recovery.md#monitoring-recoveries) describes what to alert on.
Message-failure logs include routing context and a truncated payload. Unexpected exceptions and
exits also appear through process failure reports. A quiet error log alone does not establish
readiness or prove that handlers have completed their work.

## Internals

| Module | Responsibility | Runs in |
| --- | --- | --- |
| `UOF.SDK` | Supervisor and public API | The host application's supervisor |
| `UOF.SDK.Config` | Validates configuration and derives the Pulsar wiring | The caller, at startup |
| `UOF.SDK.Pulsar` | Builds the shared client and the two subscription specs from `:pulsar` | The caller, at startup |
| `UOF.SDK.SystemPipeline` | Reads the system subscription: `alive`, `snapshot_complete`, and consumer tags go to the monitor; event content is acknowledged undecoded | Broadway, one processor |
| `UOF.SDK.ContentPipeline` | Reads the content subscription: decodes event messages and calls the handler | Broadway, `:concurrency` processors |
| `UOF.SDK.MessageMetadata` | Reads the connector's message properties | The pipelines |
| `UOF.SDK.RoutingKey` | Parses routing keys: message type, event identity, node filtering | The pipelines |
| `UOF.SDK.ProducerMonitor` | The control plane: routes observations, applies ownership and session readiness, persists transitions, publishes status callbacks | Its own process |
| `ProducerMonitor.Producer` | Per-producer state machine: health, recovery jobs, cooldowns, and request attempts; `advance/1` decides when a request is due | The monitor; each HTTP request runs in a linked task |
| `ProducerMonitor.Session` | Tracks the upstream consumer tag, independently of producer state | The monitor |
| `ProducerMonitor.Snapshot` | The persisted state and its encoding | The monitor |
| `ProducerMonitor.Store` | Persistence behaviour; `Store.ETS` is the default | The monitor; a store with `child_spec/1` also runs its own process |

A recovery request's response returns to the monitor as a message. A response for a job
completed, parked, or replaced meanwhile is dropped, and the superseded request is left to
finish rather than shut down: it has usually reached Betradar already, and a duplicate request
is harmless beyond quota.

Useful entry points are the [SDK supervisor](../lib/uof/sdk.ex),
[Pulsar wiring](../lib/uof/sdk/pulsar.ex),
[producer monitor](../lib/uof/sdk/producer_monitor.ex), and
[store behaviour](../lib/uof/sdk/producer_monitor/store.ex).

## Design Invariants

Changes must preserve these rules; each links to the section that explains it.

1. **The monitor starts before the consumers that report to it.** Consumers report ownership
   and sessions to its registered name, and `:rest_for_one` restarts them with a restarted
   monitor so it receives fresh reports. See [the supervision tree](#the-supervision-tree).
2. **The system subscription is the only control plane.** Only the system pipeline reports to
   the monitor; the content pipeline never does. See [feed continuity](#feed-continuity).
3. **Only the active system-subscription owner issues recovery requests.** A passive instance
   never sees the `snapshot_complete` for a request it issued, so it would burn recovery quota
   reissuing it on every stall.
   See [ownership and failover](#ownership-and-failover).
4. **Recovery intent is persisted before its request is issued**, so a crash cannot resume
   from stale synchronized state. See [snapshot contents](#snapshot-contents).
5. **Checkpoints only move forward**, so an out-of-order heartbeat cannot move one back and
   cause needless replays and writes. See [snapshot contents](#snapshot-contents).
6. **Monitor state is one snapshot per subscription, replaced atomically and written only by
   the active owner**, and an unreadable snapshot fails startup rather than discarding
   checkpoints. A passive instance's copy is stale. See [persistence](#persistence).
7. **A recovery completes only on a `snapshot_complete` matching its active request ID.**
   Completions for older, superseded, or foreign requests are ignored. See
   [how a recovery runs](recovery.md#how-a-recovery-runs).
8. **The content subscription exists before the first recovery.** This is a deployment
   requirement the SDK relies on, not one it can enforce. See
   [subscriptions](deployment.md#subscriptions).
9. **Content callbacks own application effects; the SDK owns feed delivery and recovery
   orchestration.** See [handler execution and delivery](#handler-execution-and-delivery).

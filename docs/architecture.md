# Architecture

- [Overview](#overview): public boundaries and the supervision tree
- [Pulsar](#pulsar): the topic, subscriptions, and feed continuity
- [Message processing](#message-processing): ordering, delivery, and failures
- [Recovery](#recovery): startup, lifecycle states, and time controls
- [Persistence](#persistence): the monitor store and restart resume
- [Observability](#observability): health and telemetry signals
- [Contributing](#contributing): internals and design invariants

## Overview

### Public Boundaries

Applications interact with a small set of modules:

| Module | Responsibility |
| --- | --- |
| `UOF.SDK` | Starts the SDK and exposes producer state and manual recovery |
| `UOF.SDK.MessageHandler` | Defines callbacks for feed content and producer-status changes |
| `UOF.SDK.ProducerStatus` | Reports a producer's lifecycle state and observations |
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

The Pulsar client is shared by both pipelines. Each pipeline
is a Broadway supervision tree with one producer stage. System processing uses one
processor; content processing uses the configured processor concurrency.

A monitor restart also restarts the Pulsar client and both pipelines. The system pipeline
must report the current consume session to the new monitor, and its consumer must report
its ownership again. A store-process restart also restarts the components that loaded its state.
A content-pipeline restart does not require restarting the system pipeline or monitor.
Failures handled inside a child, such as a broker reconnect, need not restart its siblings.

The SDK uses registered names for its monitor, pipelines, and Pulsar client. The standard
configuration starts one SDK instance per VM; scale-out uses multiple such instances.

## Pulsar

Betradar delivers UOF over AMQP. A RabbitMQ source connector consumes it into a Pulsar
topic, which introduces durable buffering between the upstream feed connection and SDK
consumers. The [README](../README.md#deployment) lists what a deployment needs and shows
how to configure the SDK; this section describes how it works.

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
pipeline filters out event content before decoding it. The system subscription is the
only control plane; see [feed continuity](#feed-continuity).

### Connector Contract

The connector publishes the original XML payload and keeps routing metadata separate from
the key used for distributed dispatch:

| Field | Purpose |
| --- | --- |
| Pulsar message key | Sport-event URN for event messages; `system` for `alive` and `snapshot_complete` |
| `__rabbitmq_routing_key` property | Original AMQP routing key, used by the SDK for message type, event identity, and node filtering |
| `__rabbitmq_consumer_tag` property | The upstream session's [consumer tag](#consumer-tags), used to detect possible gaps and to resume after an SDK restart |

Use a RabbitMQ source connector with `messageKeyRules` and `__rabbitmq_routing_key`
support ([apache/pulsar-connectors#140](https://github.com/apache/pulsar-connectors/pull/140)).
Configure these rules in its source configuration; the first full match wins, and
unmatched keys retain the original routing key:

```json
{
  "messageKeyRules": [
    {
      "regex": "^[^.]*\\.[^.]*\\.[^.]*\\.(?:alive|snapshot_complete)(?:\\..*)?$",
      "replacement": "system"
    },
    {
      "regex": "^(?:[^.:]*\\.)+?([^.:]+:[^.:]+)\\.([^.\\-][^.]*)(?:\\..*)?$",
      "replacement": "$1:$2"
    }
  ]
}
```

These are JSON rules for the connector, not SDK consumer options. See
[`docker-compose.yml`](../docker-compose.yml) for the complete local source setup
(with the extra dollar-sign escaping required by Compose). Do not configure a fixed
consumer tag: each new upstream consume session must have a distinct server-generated
tag (`amq.ctag-…`) so the SDK can detect reconnects.

> [!WARNING]
> If batching is enabled for the RabbitMQ source connector's Pulsar producer, use
> key-based batching (`KEY_BASED`). Alternatively, disable producer batching.
> Default batching can combine different events and system messages in one batch, which Pulsar
> routes using the first message's key. This breaks the Key-Shared distribution
> semantics required by the content subscription. Configure this on the connector's
> producer; SDK consumer configuration cannot repair mixed-key batches. See
> [Pulsar's Key-Shared producer requirements](https://pulsar.apache.org/docs/4.2.x/concepts-messaging/).

For the connector CLI, select key-based batching with
`pulsar-admin sources create --batch-builder KEY_BASED`.

### Ownership and Failover

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

### Feed Continuity

With one partition and a Failover subscription, the active system consumer receives every
message on the topic, including content it acknowledges without decoding. A delivery gap
can only start upstream of the topic, when the connector's consume session changes, and
every message carries that session's [consumer tag](#consumer-tags). Content not yet
processed stays in the content subscription's backlog. The system pipeline alone
therefore decides readiness, session changes, checkpoints, and recovery; the content
pipeline does not report to the monitor.

Producer status describes feed continuity on the topic, not application progress:

- `:up` means the topic carries a continuous feed for the producer. Content may still be
  waiting in the content subscription.
- Content lag is not tracked. With Key-Shared, one monitor would see only its own share of
  content anyway. Use handler timestamps or the content subscription's backlog statistics
  to measure processing lag across instances.
- The checkpoint advances with subscribed system heartbeats while `:up`, regardless of how
  far content processing has got. A recovery after a session change replays from there;
  everything earlier is already on the topic.

This relies on the content cursor never losing a message the system subscription has
seen. Once the content subscription exists, its backlog holds everything it has not
acknowledged: see [backlog and restart continuity](#backlog-and-restart-continuity), and do
not skip, reset, or expire its cursor.

Before the content subscription exists, nothing protects the messages the system
subscription acknowledges. Pulsar subscribes asynchronously, so on a first start, or after
the content subscription is deleted, the system pipeline can request a recovery while the
content consumer is still connecting. If the topic has no retention (the broker default),
messages acknowledged by every existing subscription can be deleted once their ledger rolls
over, and a content subscription created afterwards misses part of the replay while the
monitor sees it complete. Use one of these:

- **Create both subscriptions before the SDK first starts** (recommended). The content
  cursor then exists before any recovery, whatever the retention. For
  `subscription: "uof-sdk"`:

  ```sh
  pulsar-admin topics create-subscription persistent://public/default/uof-feed \
    --subscription uof-sdk-content --messageId earliest
  pulsar-admin topics create-subscription persistent://public/default/uof-feed \
    --subscription uof-sdk-system
  ```

- **Or set a retention time on the topic** longer than the content subscription can take to
  be created after the SDK starts, for example one hour. Topic policies must be enabled on
  the broker; `pulsar-admin namespaces set-retention` sets the same policy for a namespace:

  ```sh
  pulsar-admin topicPolicies set-retention persistent://public/default/uof-feed \
    --time 1h --size -1
  ```

  The SDK starts a new content subscription at the earliest retained message
  (`initial_position: :earliest`, overridable through `:consumer_opts`), so it still reads
  what the system subscription has acknowledged. Retention also sets how much a new content
  subscription processes before it reaches current traffic: one hour covers a consumer in
  connection backoff, while a day of retention means a new deployment replays a day of
  feed. The producers' recovery windows do not bound this interval.

This differs from the official SDKs, which tie the checkpoint to content progress. The
Java SDK ([`e677b92`](https://github.com/sportradar/UnifiedOddsSdkJava/tree/e677b9222bf59b770384379f62f789778ddc8053))
advances it from system-session alives only while the producer is not flagged down, and
flags it down when processing lags (`ProcessingQueueDelayViolation`). The .NET SDK
([`a6045c1`](https://github.com/sportradar/UnifiedOddsSdkNetCore/tree/a6045c1d255c0b1510a0e70f0696b7e140645303))
uses the oldest alive seen on the user sessions. The
[recovery documentation](https://docs.sportradar.com/uof/sdk/features/recovery) recommends
storing the last processed message timestamp, and states that slow processing marks a
producer down without issuing recovery. All three consume AMQP directly, where unprocessed
content is lost with its queue. The protocol only requires recovering from a timestamp
before which nothing was missed; here that boundary is the topic, not processing.

### Node Isolation

The source connector binds `#`, so a topic can contain recovery traffic for several
subscriptions using the same access token. Both SDK pipelines inspect the original routing
key and acknowledge messages addressed to another node without decoding them, invoking a
handler, or updating the monitor. Unaddressed messages (`-` in the node position) are
accepted by every node. A `nil` or non-positive `node_id` disables this filtering.

`node_id` identifies a subscription, not an individual SDK instance:

- All instances sharing a base subscription must use the same positive `node_id`.
  Otherwise Key-Shared can deliver part of a recovery replay to instances that discard it,
  while the active system consumer still accepts `snapshot_complete`: a silent gap.
- Distinct base subscriptions using the same access token need distinct positive IDs to
  isolate their recovery replays and `snapshot_complete` messages.
- Use `nil` only when a single subscription consumes the access token.

### Backlog and Restart Continuity

The connector is outside the SDK supervision tree. Restarting an SDK instance therefore
does not necessarily restart the upstream AMQP session. A durable subscription can retain
messages while that instance is offline; retention settings must preserve the backlog needed
for the deployment's restart behaviour. Disable message TTL or set it above the worst-case
downtime, and use a `producer_exception` backlog quota policy to prevent eviction.
Retention, which keeps messages after every subscription has acknowledged them, is
separate; see [feed continuity](#feed-continuity) for when it is needed.

## Message Processing

Content follows a short path:

```text
Pulsar delivery
    → routing-key property
    → Broadway partition dispatch
    → node filtering
    → XML decoding
    → application callback
    → Pulsar acknowledgement
```

### Ordering and Scale-Out

Within an instance, the content pipeline partitions messages by sport-event URN. Messages
for one event go to the same processor, while different events can be handled concurrently.
System messages use a separate pipeline so producer monitoring has its own processing lane.
There is no processing-completion ordering between the system and content pipelines.

With the [connector rules above](#connector-contract), Key-Shared dispatch and local
Broadway partitioning use the same event identity. Different message types for one event,
such as `odds_change` and `bet_stop`, reach the same SDK instance and the same local
processor. Using the full routing key instead would split those message types across
instances and defeat per-event ordering. Key-based batching is part of this contract.

This preserves the per-event processing lane, not exactly-once delivery or a total order
across events. Reconnects, redelivery, and recovery can replay messages. Applications must
still handle duplicates and stale updates, and preserve ordering themselves if they offload
work from the synchronous callback.

The callbacks themselves are listed in the [README](../README.md#handler-reference).

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
Store calls also execute in the monitor, so their latency affects when it can process the
next observation or timer message. Recovery requests and producer-description reloads run in
tasks linked to the monitor: a slow API does not block it, and an exception from the request
still fails it. The startup description fetch is the exception; it runs before the monitor
takes any observations.

Handler persistence and Pulsar acknowledgement are separate operations. Applications
should make their effects idempotent because reconnects and recovery can replay messages.
Failures pass through the pipeline's failure callback, which logs context and emits
`[:uof_sdk, :message, :failed]` telemetry. The pipeline nacks a failed message and, about a
second later, Pulsar diverts it to
the `<topic>-<subscription>-DLQ` topic instead of processing it again, so it remains available
for inspection; batch-index acknowledgement keeps the failure from redelivering the other
messages of its batch. Override `:consumer_opts` (`:redelivery_interval`,
`:dead_letter_policy`, `:batch_index_ack_enabled`) to change this.
A message failure emits diagnostics but does not directly request producer recovery.

Content delivery is not gated on producer health. Live and replayed content can reach handlers
while a producer is `:down` or `:recovering`, and content can still be waiting in the backlog
while it is `:up`. The SDK reports synchronization and
health; applications decide how those reports affect their business operations. A producer
becoming `:up` is not a barrier proving that all application side effects have completed.

Raw system messages are internal inputs. `alive` observations update producer health and
recovery progress, and `snapshot_complete` correlates recovery completion. Applications
receive lifecycle changes through `handle_producer_status/1` and can query state through
`UOF.SDK.producer/1` or `UOF.SDK.producers/0`.

## Recovery

Starting the supervisor establishes the runtime tree. Producer initialization loads persisted
session/progress records and fetches active producer descriptions through `uof_api`; recovery
needs each producer's API product and advertised recovery window. Fetched descriptions are
stored in the monitor snapshot. If the API fails, the monitor starts from the stored
descriptions, or else from the configured `:producers`, logs a warning, and keeps retrying
the API in the background. Startup fails only when none of the three has descriptions. An
`alive` from an unknown producer also reloads the descriptions, at most once per minute, so a
producer activated after startup is monitored from its next `alive`. Feed consumption
and producer synchronization proceed after that initialization; a successful `start_link/1`
is not a feed-synchronization barrier.

Recovery is a coordinated sequence:

1. An initial synchronization need, session change, health observation, or manual request
   creates recovery intent for a producer.
2. The monitor persists that the producer requires recovery before issuing HTTP.
3. Once ownership, connection gates, and cooldown permit it, the SDK requests recovery with
   a request ID. After a heartbeat timeout it also waits for the producer's alives to
   resume: Betradar rejects recovery for a silent producer, and failed requests count
   against its rate limit. A stored checkpoint selects incremental recovery, whose replay
   start is clamped to the recovery window when issued; without one, recovery requests a
   full snapshot.
4. Replayed content arrives through the normal feed and application callbacks.
5. A matching `snapshot_complete` ends the recovery job and updates producer state.

Each producer draws request IDs from its own sequence, which starts at a random offset, so a
completion for a request issued before a VM restart or by a previous control-plane owner is
unlikely to match a newer request. Request IDs are correlation values, not fencing tokens.

Rejected or failed HTTP requests (an API error, an HTTP transport error, or a non-`ACCEPTED`
response) are retried after the recovery interval. An exception raised while issuing the
request is not treated as a rejection: it fails the monitor, and the persisted recovery
intent makes the restarted monitor recover again. A stall timer reissues a request
whose completion has not arrived, retaining its original recovery timestamp. Superseded retry
messages are ignored using recovery-job generations.

`snapshot_complete` marks the feed's completion of a recovery replay. Content callbacks run
on their own processors, so this system observation is separate from application processing
completion. A producer is `:up` when the topic carries a continuous feed for it, which
describes [feed continuity](#feed-continuity), not application progress: replayed content
can still be waiting in the content subscription. Applications that must not act until
every replayed message is handled measure that themselves.

The lifecycle states describe the monitor's view:

| State | Meaning |
| --- | --- |
| `:down` | Unsynchronized: no recovery job yet, or one waiting for the producer's alives to resume |
| `:recovering` | Recovery is pending, being requested, or awaiting completion |
| `:up` | The monitor considers the producer synchronized |
| `:resuming` | Restored progress is eligible for resume, pending continuity checks |

A pending or in-flight recovery job is reported as `:recovering` in `UOF.SDK.ProducerStatus`.
Recovery jobs, timers, and request correlation are not part of it.

Query the current state synchronously:

```elixir
UOF.SDK.producers()
#=> [%UOF.SDK.ProducerStatus{id: 1, product: "liveodds", status: :up, ...}, ...]

UOF.SDK.producer(1)
#=> {:ok, %UOF.SDK.ProducerStatus{...}}
```

Automatic recovery normally needs no application intervention. For a manual request, use
`UOF.SDK.recover(1)`; pass `full: true` to ignore the checkpoint and request a full snapshot.
This uses the same monitor coordination, lifecycle reporting, and stall guard as automatic
recovery. On a passive instance it returns `{:error, :passive}`; issue the request
on the active owner instead.

### Consumer Tags

RabbitMQ assigns a consumer tag (`amq.ctag-…`) to every consume session. The source
connector copies its session's tag into the `__rabbitmq_consumer_tag` property of every
message. A new tag means the connector's upstream session was re-established and messages
may have been missed. Both subscriptions read the same session from the same topic, so the
system pipeline alone reports the tag, from each `alive`.

While running, a tag the monitor has not seen before recovers every producer. Tags are
compared by novelty rather than arrival order, so a late message from a superseded session
is not a change.

The monitor saves the last tag in its [snapshot](#snapshot-contents). After a restart it
waits, for up to `inactivity_seconds`, until the system pipeline reports the current tag.
A matching tag means the upstream session never broke: synchronized producers resume from
the retained backlog without recovery. This is what lets a deployment survive an SDK
restart. A different tag is saved with every producer unsynchronized, and recovery starts.

### Time and Recovery Controls

Public duration settings are in seconds and converted to milliseconds internally. Feed
timestamps and stored checkpoints are epoch milliseconds. Heartbeat inactivity uses local
observation time, and the recovery window is applied against the local wall clock. Recovery
cooldown uses monotonic time.

| Setting | What it measures or controls | Effect |
| --- | --- | --- |
| `inactivity_seconds` | Elapsed time since a system heartbeat or matching recovery completion established liveness | Records recovery on an observed heartbeat timeout and requests it once the producer's alives resume; also bounds session-readiness waiting |
| `min_interval_between_recoveries` | Per-producer recovery-request cooldown and delay after a failed request | Defers requests or retries while recovery remains pending |
| `max_recovery_time` | Wait for matching `snapshot_complete` after a successful recovery request | Reissues a stalled recovery while retaining its original replay timestamp |
| `recovery_overlap_seconds` | Look-back subtracted from a stored checkpoint | Replays overlapping data during incremental recovery, bounded by the producer's recovery window |

These controls have distinct purposes: recovery overlap is not a timeout, and a stalled
recovery is not a heartbeat failure. Health checks and timer messages are handled by the
monitor, subject to control-plane ownership and its current recovery state; durations are
not hard real-time deadlines. See [configuration](../README.md#configuration) for defaults and
[startup and recovery](#recovery) for the recovery sequence and manual requests.

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
| `consumer_tag` | Committed [consumer tag](#consumer-tags) of the connector's upstream session |
| `producers` | Per producer: recovery checkpoint (epoch milliseconds) and whether it is synchronized with that session |

A producer may resume at startup only when it is synchronized and has a checkpoint. A
consumer tag change saves the new tag with every producer unsynchronized, in one write. Recovery preparation saves the producer as unsynchronized before its HTTP request,
so a crash at any point cannot resume from stale state. Checkpoints only move forward.

The monitor saves only when the snapshot changes: on a session change, a checkpoint advance
from a subscribed system heartbeat while `:up`, and a producer's synchronization change.

`load/1` returns `nil` only when nothing has been saved, and the monitor then starts every
producer `:down`. A stored binary that cannot be decoded fails startup: discarding it would
lose the checkpoints that bound incremental recovery.

### Implementing a Store

A store implements `load(opts)` and `save(snapshot, opts)`, and optionally `child_spec/1`.
Configure it as a module or `{module, opts}`; `opts` is passed to every callback:

```elixir
config :uof_sdk, monitor_store: {MyApp.MonitorStore, repo: MyApp.Repo, key: "uof-node-a"}
```

`save/2` must replace the snapshot atomically and return `:ok` only once the write is
durable. Let backend failures raise or exit; never return `nil` for a failed read or `:ok`
for a failed write. Callbacks run in the monitor, so a slow backend delays health checks and
`UOF.SDK.producers/0`; set backend timeouts accordingly. A failed write crashes the monitor,
and a backend that stays unavailable eventually stops `UOF.SDK` through supervisor restart
limits.

Each snapshot has exactly one writer. Monitors sharing a backend, including SDK instances on
one Pulsar subscription, need distinct keys that are stable across restarts, such as a
StatefulSet pod name. A key that changes on restart never resumes.

For example, a PostgreSQL store using Ecto:

```elixir
# migration
create table(:uof_sdk_monitor_snapshots, primary_key: false) do
  add :key, :string, primary_key: true
  add :snapshot, :binary, null: false
end
```

```elixir
defmodule MyApp.MonitorStore do
  @behaviour UOF.SDK.ProducerMonitor.Store

  import Ecto.Query

  @table "uof_sdk_monitor_snapshots"

  @impl true
  def load(opts) do
    {repo, key} = {Keyword.fetch!(opts, :repo), Keyword.fetch!(opts, :key)}
    repo.one(from(s in @table, where: s.key == ^key, select: s.snapshot), timeout: 2_000)
  end

  @impl true
  def save(snapshot, opts) do
    {repo, key} = {Keyword.fetch!(opts, :repo), Keyword.fetch!(opts, :key)}

    {1, _} =
      repo.insert_all(@table, [%{key: key, snapshot: snapshot}],
        on_conflict: {:replace, [:snapshot]},
        conflict_target: :key,
        timeout: 2_000
      )

    :ok
  end
end
```

Start the repo before the SDK. A store that owns a process implements `child_spec/1`; the
SDK starts `{module, opts}` as its first child, and a store-process restart also restarts the
monitor and consumers through `:rest_for_one`.

Test a persistent store for an empty load, replacement, key isolation, and visible failures.
The [ETS store tests](../test/uof/sdk/producer_monitor/store/ets_test.exs) show the contract.

### Restart Resume

At startup, a synchronized producer with a checkpoint enters `:resuming`. It becomes `:up`
once the system pipeline reports an unchanged [consumer tag](#consumer-tags) and a current
heartbeat arrives. Without a heartbeat within `inactivity_seconds` it recovers, and a
readiness deadline prevents an incomplete restart from waiting indefinitely.

The SDK resumes retained backlog when the connector's upstream session remains unchanged.
An upstream session change or recovery-required heartbeat still enters the normal recovery
path. Persistence alone does not prove delivery continuity or replace broker backlog
retention.

Incremental recovery subtracts the configured overlap from the checkpoint and clamps the
result to the producer's advertised recovery window. Handlers must tolerate replayed
messages. See [store implementation](#implementing-a-store) for the
configuration and durability contract, and [backlog requirements](#backlog-and-restart-continuity)
for the broker side of restart continuity.

## Observability

A running supervisor, an established Pulsar connection, and a synchronized producer are
separate states. Use producer status to understand feed synchronization; measure processing
lag from handler timestamps or the content subscription's backlog, and use Pulsar and
message diagnostics to investigate why progress has stopped.

| Signal | What it tells the application |
| --- | --- |
| `UOF.SDK.producers/0` and `UOF.SDK.producer/1` | Current per-producer lifecycle state and observations |
| `handle_producer_status/1` | Reported producer-state transitions, delivered synchronously by the monitor |
| `[:uof_sdk, :message, :failed]` | A decoding or processing failure, with routing context and failure reason |
| `[:uof_sdk, :recovery, :initiated]` | A successful recovery request, with producer ID, request ID, and replay start timestamp; not recovery completion |
| `[:uof_sdk, :recovery, :completed]` | A matching `snapshot_complete`, with `duration_ms` since the completed request was issued |
| `[:uof_sdk, :recovery, :failed]` | A rejected or failed recovery request, with its reason and `retry_in_ms` |
| `[:uof_sdk, :recovery, :stalled]` | A request with no `snapshot_complete` within `max_recovery_time`, emitted before it is reissued |
| `[:uof_sdk, :producer, :status]` | A reported producer status transition, emitted whether or not a handler is configured |
| `[:uof_sdk, :producers, :loaded]` | Producer descriptions loaded, with `count` and `source` (`:api`, `:snapshot`, or `:config`); a fallback source means the API was unavailable |
| `[:uof_sdk, :message, :other_node]` | A message addressed to another `node_id`, acknowledged without processing; expected when subscriptions share an access token |
| `[:pulsar, :consumer, ...]` | Consumer events from the `pulsar` client, such as nacks, redelivery requests, and dead-letter diversions |

Recovery events share producer ID, product, and request ID, so initiation, failures,
stalls, and completion can be correlated per request. A recovery with no `:completed`
event, or repeated `:failed` or `:stalled` events, is the signal to alert on. Message-failure logs include routing context and
a truncated payload. Unexpected exceptions and exits also appear through process failure
reports. A quiet error
log alone does not establish readiness or prove that handlers have completed their work.

## Contributing

`ProducerMonitor` is the coordination GenServer. It routes observations, applies ownership
and connection gates, persists transitions, and publishes producer-status callbacks.
`ProducerMonitor.Producer` owns per-producer health transitions, recovery jobs, cooldowns,
HTTP attempts, and timers. These functions execute in the monitor process, except the HTTP
request itself, which runs in a linked task whose response returns as a message; a response
for a job completed, parked, or replaced meanwhile is dropped. Such a superseded request is
left to finish rather than shut down: it has usually reached Betradar already, and a
duplicate request is harmless beyond quota. Status callbacks run in the monitor and should
complete promptly.

`ProducerMonitor.Connections` tracks [consumer tags](#consumer-tags) independently of producer state.
`Transport` derives the two producer specifications and the shared Pulsar client from the
`:pulsar` configuration. `MessageMetadata` reads the connector's message properties, and `RoutingKey` supplies
routing and event identity. The pipelines own decoding and dispatch rather than recovery
policy.

Useful entry points are the [SDK supervisor](../lib/uof/sdk.ex),
[Pulsar wiring](../lib/uof/sdk/transport.ex),
[producer monitor](../lib/uof/sdk/producer_monitor.ex), and
[store behaviour](../lib/uof/sdk/producer_monitor/store.ex).

### Design Invariants

1. The monitor starts before the consumers that report sessions or ownership to it.
2. The system subscription is the only control plane; the content pipeline never reports to the monitor.
3. Only the active system-subscription owner issues recovery requests.
4. Recovery intent is persisted before its HTTP request is issued.
5. Monitor state is persisted as one snapshot, replaced atomically; an unreadable snapshot fails startup.
6. Content callbacks own application effects; the SDK owns feed delivery and recovery orchestration.

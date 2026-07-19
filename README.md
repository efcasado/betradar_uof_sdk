# Betradar UOF SDK

> [!IMPORTANT]
> This is an **unofficial Elixir SDK** for Betradar's Unified Odds Feed (UOF).
> Betradar offers official Java and .NET SDKs. You can read more about them at
> <https://sdk.sportradar.com>.

An Elixir SDK for Betradar's [Unified Odds Feed](https://docs.betradar.com/)
(UOF). It connects to the feed, decodes XML messages into structs, sends those
messages to a handler you implement, and keeps each producer in sync with
automatic recovery.

The SDK is built on [Broadway](https://hexdocs.pm/broadway) and depends on
[`uof_api`](https://hex.pm/packages/uof_api) and
[`uof_schemas`](https://hex.pm/packages/uof_schemas).

The SDK handles:

- AMQP or Pulsar feed consumption.
- XML decoding into Elixir structs.
- Message delivery to your application callbacks.
- Producer health tracking.
- Recovery after startup, reconnects, or feed gaps.

Your application handles:

- Business logic for odds changes, settlements, bet stops, and other feed
  messages.
- Persistence of the data you care about.
- Idempotency for replayed or duplicated messages.
- Persistent checkpoint storage in production.

## Installation

Add the SDK and one transport dependency to your application.

For AMQP:

```elixir
def deps do
  [
    {:uof_sdk, "~> 0.1.0"},
    {:broadway_rabbitmq, "~> 0.8"}
  ]
end
```

For Pulsar:

```elixir
def deps do
  [
    {:uof_sdk, "~> 0.1.0"},
    {:off_broadway_pulsar, "~> 1.5"}
  ]
end
```

## Quick start

The fastest way to verify a connection is to use the built-in log handler.

Configure one transport with your Betradar access token and virtual host:

```elixir
config :uof_sdk,
  handler: UOF.SDK.LogHandler,
  node_id: 1,
  transport: {:amqp,
    connection: [
      host: "stgmq.betradar.com",
      username: System.get_env("UOF_ACCESS_TOKEN"),
      password: "",
      virtual_host: "/unifiedfeed/12345",
      ssl_options: []
    ]
  }

config :uof_api,
  base_url: "https://stgapi.betradar.com/v1",
  auth_token: System.get_env("UOF_ACCESS_TOKEN")
```

Start the SDK manually:

```elixir
UOF.SDK.start_link([])
```

The SDK connects to the feed, monitors producer health, performs recovery when
needed, and logs incoming messages through `UOF.SDK.LogHandler`.

## Configuration

> [!NOTE]
> Configure one transport. The SDK derives the separate Broadway producers it
> needs for content and system messages.

The transports have different scaling models in this SDK. Pulsar supports
horizontally distributed content processing: instances sharing a subscription
divide messages through a Key-Shared subscription, while one instance owns
system processing through Failover. The direct AMQP transport creates an
exclusive queue per SDK instance, so instances do not share the workload; it
is intended primarily for single-instance deployments with local Broadway
concurrency. This distinction applies to the SDK's transport configuration,
not to RabbitMQ and Pulsar generally.

### AMQP

```elixir
config :uof_sdk,
  handler: MyApp.FeedHandler,
  node_id: 1,
  transport: {:amqp,
    connection: [
      host: "stgmq.betradar.com",
      username: System.get_env("UOF_ACCESS_TOKEN"),
      password: "",
      virtual_host: "/unifiedfeed/12345",
      ssl_options: []
    ]
  }

config :uof_api,
  base_url: "https://stgapi.betradar.com/v1",
  auth_token: System.get_env("UOF_ACCESS_TOKEN")
```

`transport: {:amqp, connection: [...]}` is passed to
[`BroadwayRabbitMQ.Producer`](https://hexdocs.pm/broadway_rabbitmq/BroadwayRabbitMQ.Producer.html)
for both pipelines, with SDK-owned bindings for content and system traffic.

Known Betradar AMQP hosts:

| Environment | Host |
|-------------|------|
| Production | `mq.betradar.com` |
| Integration | `stgmq.betradar.com` |
| Replay | `replaymq.betradar.com` |

### Pulsar

For Pulsar, configure one topic and base subscription. The SDK derives the
content subscription as Key-Shared and the system subscription as Failover.

```elixir
config :uof_sdk,
  handler: MyApp.FeedHandler,
  node_id: 1,
  transport: {:pulsar,
    host: "pulsar://localhost:6650",
    topic: "uof-feed",
    subscription: "uof-sdk"
  }
```

Pulsar support assumes the SDK's RabbitMQ source connector contract:

- The RabbitMQ source connector produces to a Pulsar topic with a single
  partition (a non-partitioned topic, or a partitioned topic with exactly one
  partition). Failover subscription ownership is assigned per partition, so
  with multiple partitions different SDK instances can each own a slice of the
  system subscription — `alive` and `snapshot_complete` messages then split
  across instances and no single instance holds the control plane (producer
  health tracking and recovery triggering).
- The AMQP routing key is published as the Pulsar message key.
- The original XML body is published as the Pulsar payload.
- `__rabbitmq_consumer_tag` is published as a message property and is a
  server-generated consumer tag (`amq.ctag-…`), unique per consume session. A
  connector that pins a fixed consumer tag blinds reconnect detection.

The SDK uses the consumer tag as a reconnect token and triggers recovery when
it changes: a new tag means a new upstream consume session, so a delivery gap
was possible. The AMQP transport uses its own consumer tag the same way.

#### Multi-instance control plane

With multiple SDK instances on the same subscription, the broker elects one
instance as the Failover system-subscription owner and notifies each instance
of its role. The SDK wires that signal to `UOF.SDK.ProducerMonitor`: only the
active instance runs periodic producer health transitions and issues recovery
requests, while passive instances keep consuming their Key-Shared content
share. A recovery-required alive racing demotion is retained but not issued
until promotion. On promotion, the new owner recovers each producer from its
own last-known checkpoint. The signal is best-effort, not a fencing mechanism:
briefly-overlapping actives can issue a duplicate recovery request, which
wastes recovery quota but is otherwise harmless.
`UOF.SDK.recover/2` returns `{:error, :passive}` on a standby instance.
Pulsar monitors start passive and wait for the initial broker ownership report
before issuing recoveries. AMQP monitors start active and do not use ownership
callbacks.

### Options

| Option | Default | Notes |
|--------|---------|-------|
| `:handler` | Required | Your `UOF.SDK.MessageHandler` module |
| `:transport` | `:amqp` | `{:amqp, opts}` or `{:pulsar, opts}` |
| `:node_id` | `nil` | Scopes AMQP bindings and recovery `snapshot_complete` per client |
| `:monitor_store` | `UOF.SDK.ProducerMonitor.Store.ETS` | Session and producer-progress persistence |
| `:concurrency` | `10` | Broadway processor concurrency per feed session |
| `:inactivity_seconds` | `20` | Alive-gap threshold before a producer is marked down and recovered |
| `:max_processing_delay_seconds` | `20` | Consumer-lag threshold before a producer becomes `:delayed` |
| `:min_interval_between_recoveries` | `30` | Recovery cooldown in seconds |
| `:max_recovery_time` | `3600` | Stall deadline before reissuing recovery in seconds |
| `:recovery_overlap_seconds` | `300` | Seconds subtracted from the stored checkpoint when requesting incremental recovery |

> [!NOTE]
> Recovery throttling defaults follow the official SDK guidance.
> `:recovery_overlap_seconds` is specific to this SDK and should be tuned for
> your deployment.

## Implementing a handler

`UOF.SDK.LogHandler` logs every message and producer-status change. Use it for a
first connection, then switch to your own handler when you are ready to process
messages.

To implement a handler, `use UOF.SDK.MessageHandler`. It provides no-op defaults
for every callback, so override only the callbacks your application needs.

```elixir
defmodule MyApp.FeedHandler do
  use UOF.SDK.MessageHandler

  @impl true
  def handle_odds_change(odds_change, ctx) do
    # ctx.producer_id, ctx.event_urn, ctx.routing_key
    :ok
  end

  @impl true
  def handle_bet_settlement(settlement, _ctx), do: :ok

  @impl true
  def handle_producer_status(producer) do
    # producer.status
    :ok
  end
end
```

Every callback except `handle_producer_status/1` receives:

- The decoded feed struct.
- A `UOF.SDK.Context` with `producer_id`, `event_urn`, `routing_key`, and
  `message_type`.

Common callbacks:

| Callback | Source / concept |
|----------|------------------|
| `handle_odds_change/2` | [Odds Change](https://docs.sportradar.com/uof/data-and-features/messages/event/odds-change) |
| `handle_bet_settlement/2` | [Bet Settlement](https://docs.sportradar.com/uof/data-and-features/messages/event/bet-settlement) |
| `handle_bet_stop/2` | [Bet Stop](https://docs.sportradar.com/uof/data-and-features/messages/event/bet-stop) |
| `handle_bet_cancel/2` | [Bet Cancel](https://docs.sportradar.com/uof/data-and-features/messages/event/bet-cancel) |
| `handle_rollback_bet_cancel/2` | [Rollback Bet Cancel](https://docs.sportradar.com/uof/data-and-features/messages/event/rollback-bet-cancel) |
| `handle_rollback_bet_settlement/2` | [Rollback Bet Settlements](https://docs.sportradar.com/uof/data-and-features/messages/event/rollback-bet-settlements) |
| `handle_fixture_change/2` | [Fixture Change](https://docs.sportradar.com/uof/data-and-features/messages/event/fixture-change) |
| `handle_producer_status/1` | SDK producer lifecycle state, derived from [alive](https://docs.sportradar.com/uof/data-and-features/messages/system/alive) and recovery handling |

Raw `alive` messages are consumed internally for recovery, checkpointing, and
producer health. Applications should use `handle_producer_status/1` instead of
reacting to heartbeat traffic directly. Status callbacks run only when the
producer's lifecycle `status` changes; timestamp and checkpoint updates are not
reported.

> [!WARNING]
> Keep callbacks fast. Slow handlers can delay later messages for the same
> event and may mark a producer as delayed. Offload heavy work asynchronously.

## Producer health and recovery

Producer state is available synchronously and through the
`handle_producer_status/1` callback. Both return the same `UOF.SDK.ProducerMonitor.Producer`
struct.

```elixir
UOF.SDK.producers()
#=> [%UOF.SDK.ProducerMonitor.Producer{id: 1, product: "liveodds", status: :up, ...}, ...]

UOF.SDK.producer(1)
#=> {:ok, %UOF.SDK.ProducerMonitor.Producer{...}}
```

A producer reports one lifecycle `status`:

- `:down` — not synchronized and no recovery is in flight
- `:recovering` — waiting to request, requesting, or awaiting recovery
  completion
- `:up` — synchronized and safe
- `:delayed` — the remote feed is healthy but local processing is behind
- `:resuming` — draining retained backlog after restart while awaiting
  current-session confirmation

The UOF protocol requires every producer to be recovered before its markets are
safe to act on. A gap in the message stream can happen on first connect,
reconnect, or alive heartbeat timeout. When a gap is detected, the SDK:

1. Uses the checkpoint in the producer's durable state, loaded from its store
   at startup.
2. Atomically marks the producer as requiring recovery before performing
   external I/O.
3. Calls `UOF.API.Recovery.recover/2` with a unique `request_id`.
4. Requests incremental recovery if a checkpoint exists, or full recovery if it
   does not.
5. Receives replayed messages over the same feed.
6. Marks the producer up when `snapshot_complete` arrives with the matching
   `request_id`. That system message also establishes the alive-timeout anchor.

`snapshot_complete` is handled on the system pipeline by design. It means the
feed has finished publishing a recovery replay, not that this SDK instance has
finished executing all handler callbacks for replayed content.

Local backlog is handled separately by the content lag monitor. If processed
content-queue timestamps fall behind by more than
`:max_processing_delay_seconds`, the producer becomes `:delayed` until
processing catches up. Event messages and content-session `alive` messages both
advance this lag timestamp; system `alive` messages do not.

A stall guard, configured with `:max_recovery_time`, reissues the recovery
request if no `snapshot_complete` arrives within the deadline. It preserves the
original timestamp so messages are not skipped on retry.

## Monitor state persistence

> [!NOTE]
> The default `UOF.SDK.ProducerMonitor.Store.ETS` is in-memory. Checkpoints,
> producer synchronization generations, and session state are lost on VM
> restart, so a full recovery is issued on the next start. This is fine for
> development and low-volume producers, but production applications should use
> a persistent store. The ETS store does survive monitor/pipeline crashes within
> the same VM, so crash-restarts still resume without recovering.

Checkpoints are owned by `ProducerMonitor` and advanced from subscribed system
`alive` heartbeats after the producer is already in sync. Content messages and
content-session `alive` messages do not write checkpoints directly.

On recovery, the SDK subtracts `:recovery_overlap_seconds` from the stored
checkpoint before requesting incremental recovery. This intentionally replays a
bounded amount of data to cover concurrent processing and distributed-consumer
skew. Handlers should be idempotent and tolerate duplicates.

To persist across VM restarts, implement the `UOF.SDK.ProducerMonitor.Store`
behaviour and configure it:

```elixir
config :uof_sdk, monitor_store: MyApp.ProducerMonitorStore
```

The behaviour separates the committed consume session from each producer's
durable recovery progress. The session contains the system and content tokens
plus a monotonically increasing generation. Producer progress contains its
checkpoint and the session generation in which it was last synchronized.
A producer may resume retained backlog only when it has a checkpoint and its
synchronization generation matches the current session generation.

Implement these callbacks:

- `load_session/0` and `load_producer_progress/0`
- `commit_session_change/1`, which must atomically store both tokens and
  advance the session generation
- `advance_checkpoint/2`, which must advance one producer's checkpoint
  monotonically
- `require_recovery/1`, which clears one producer's synchronized generation
- `mark_synchronized/2`, which records the current session generation for
  one producer

Advancing the session generation invalidates every producer synchronized in
an older generation without a multi-producer transaction. If the monitor
crashes after committing the new tokens but before issuing recovery, the
generation mismatch still prevents an unsafe resume. If it crashes before the
session commit, the old tokens cause the change to be detected again.

Recovery preparation is persisted before the HTTP request is issued. This
ordering is deliberate: if the monitor crashes after requesting recovery, the
next monitor must recover again rather than incorrectly resume from state that
still claimed the producer was synchronized.

Each mutation callback must atomically update the session record or one
producer record as described above. A store must also have exactly one writer:
its `ProducerMonitor`. Concurrent writes from another monitor, node, or
administration tool are unsupported and may overwrite newer state.

### Restart resume

With a persistent store, `ProducerMonitor` restores resumability state and
connection tokens at startup instead of assuming a gap. See
`UOF.SDK.ProducerMonitor`'s moduledoc ("Restart resume" section) for the full
mechanics — briefly: a producer healthy at shutdown starts as `:resuming` and
drains retained backlog instead of recovering immediately. A matching token
means the upstream consume session did not change; avoiding a gap also requires
the transport to retain the backlog.

Persisted tokens also close the restart gate: recovery intent may be recorded
immediately, but no recovery HTTP request is sent until both the system and
content pipelines report their current sessions. If either pipeline does not
become ready within `:inactivity_seconds`, the monitor crashes and the SDK's
`:rest_for_one` supervisor restarts both pipelines. A resumable producer that
never supplies a current-session alive uses the same interval before falling
back to normal recovery.

Whether a restart actually avoids recovery is decided by the transport. A
direct AMQP session always mints a new consumer tag on restart, so recovery
still fires (the exclusive queue lost messages while down). A Pulsar transport
fed by a long-lived source connector keeps the same connector tag across app
restarts, and the durable subscription retains the backlog — so deployments
restart, drain, and resume without an odds recovery. `subscribed=0` alives and
token changes replayed from the backlog still force recovery when a genuine
upstream gap happened while the app was down.

The Pulsar broker must not drop retained backlog: configure no message TTL (or
one comfortably above your worst-case downtime) and a `producer_exception`
backlog quota policy — evicted backlog is a silent gap the SDK cannot detect.

Stores that are backed by infrastructure your application already supervises,
such as an Ecto repo, do not need to be started by the SDK. Configure the store
module and make sure the repo is part of your application supervision tree
before `UOF.SDK`.

```elixir
children = [
  MyApp.Repo,
  UOF.SDK
]
```

If a store owns a process of its own, implement `child_spec/1`; the SDK will
start it before the producer monitor.

## Architecture

`UOF.SDK` is a library supervisor that starts the following components in order:

```text
UOF.SDK
|-- ProducerMonitor.Store*     - optional configured monitor-state store child
|-- ProducerMonitor            - producer health and recovery orchestration
|-- SystemPipeline             - feed consumer for alive and snapshot_complete
`-- ContentPipeline            - feed consumer for event content
```

`*` The store appears in the supervision tree only when the configured
`UOF.SDK.ProducerMonitor.Store` implementation provides `child_spec/1`.

Messages flow in one direction:

1. Pipelines receive raw AMQP or Pulsar messages.
2. The SDK decodes the XML payload.
3. The SDK calls your `MessageHandler`.

`SystemPipeline` owns system traffic such as `alive` and `snapshot_complete`.
It notifies `ProducerMonitor` for producer lifecycle, recovery correlation, and
checkpoint advancement.

`ContentPipeline` owns event content and reports content-queue timestamps for
lag detection. It also consumes session-scoped `alive` messages only as lag
freshness markers for quiet producers.

`UOF.SDK.ProducerMonitor` defines and owns its runtime state struct. It contains
the producer aggregates, connection-session state, ownership, loaded durable
session and producer-progress records, and runtime dependencies. The focused
modules own their respective transitions:

- `UOF.SDK.ProducerMonitor.Producer` is the per-producer state machine. It owns
  health observations, lifecycle transitions, and the complete recovery state:
  static request configuration, cooldown history, the optional pending/in-flight
  job, HTTP attempts, and timers.
- `UOF.SDK.ProducerMonitor.Connections` detects consume-session changes.
- `UOF.SDK.ProducerMonitor.Store.Session` identifies the committed consume
  sessions and their generation.
- `UOF.SDK.ProducerMonitor.Store.ProducerProgress` records checkpoints and the
  generation in which each producer was synchronized.
- `UOF.SDK.ProducerMonitor.Store` defines granular atomic persistence
  operations for those records.

The `ProducerMonitor` GenServer is the public coordination boundary: it routes
events, applies global ownership and connection gates, persists transitions,
and publishes status callbacks. `UOF.SDK.producers/0` reads its runtime state
through a `GenServer.call`.

For Pulsar transports, the SDK reads the original AMQP routing key from the
Pulsar partition key produced by the RabbitMQ source connector.

## Integration testing

The integration test builds the RabbitMQ source connector, starts the complete
Docker Compose environment, verifies synthetic UOF events end to end, and
cleans up afterward:

```bash
make test-integration
```

Use `make compile` and `make test-unit` for the usual development checks.

The connector repository, revision, and checkout directory can be overridden
with `RABBITMQ_SOURCE_REPO`, `RABBITMQ_SOURCE_REF`, and
`RABBITMQ_SOURCE_DIR`. Gradle builds incrementally, and CI uses the same command
with a persistent Gradle cache. The ordinary `mix test` suite excludes this
Docker-backed test.

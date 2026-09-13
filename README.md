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

`transport: {:amqp, connection: [...]}` configures one shared AMQP connection,
with a separate channel and exclusive queue for each pipeline. See the
[direct AMQP architecture](docs/architecture.md#direct-amqp) for connection
ownership, reconnection, and failure handling.

Custom AMQP producers sharing a connection must provide `:consumer_tag` metadata
or an explicit reconnect token to detect channel-only reconnects. The legacy
connection-pid fallback detects only connection replacement.

Known Betradar AMQP hosts:

| Environment | Host |
|-------------|------|
| Production | `mq.betradar.com` |
| Integration | `stgmq.betradar.com` |
| Replay | `replaymq.betradar.com` |

### Pulsar

For Pulsar, configure one topic and base subscription. Use the same topic and
base subscription across SDK instances that should share content delivery. The
SDK derives `<subscription>-content` (Key-Shared) and `<subscription>-system`
(Failover), sharing one supervised Pulsar client.

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

Use the SDK's RabbitMQ source connector to supply the feed payload and metadata.
The current SDK requires a non-partitioned topic or a topic with exactly one
partition so its system subscription has one active recovery coordinator.

See the [Pulsar architecture](docs/architecture.md#pulsar) for connector behavior,
batching, backlog retention, and failover, and
[restart resume](docs/architecture.md#restart-resume) for restart continuity.

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
> event and may mark a producer as delayed. If you offload work, make the handoff
> durable before returning when it represents completed delivery. See
> [handler execution](docs/architecture.md#handler-execution-and-delivery).

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

The SDK handles producer synchronization and recovery automatically. Applications
use producer status to decide how feed health affects their business operations;
content delivery continues during recovery. See the architecture guide for
[startup and recovery](docs/architecture.md#startup-and-recovery),
[health and operational signals](docs/architecture.md#health-and-operational-signals),
and [restart resume](docs/architecture.md#restart-resume).

## Implementing a ProducerMonitor Store backend

The store persists consume-session identity and per-producer recovery progress.
Implement `UOF.SDK.ProducerMonitor.Store` to use your application's persistent
backend, then configure it:

```elixir
config :uof_sdk, monitor_store: MyApp.ProducerMonitorStore
```

> [!NOTE]
> The default `UOF.SDK.ProducerMonitor.Store.ETS` is in-memory. Records survive
> monitor and pipeline restarts while the store process remains alive, but are
> lost when that process or the VM stops. Use a persistent backend when progress
> must survive those boundaries.

Implement the six callbacks in the [store behaviour](lib/uof/sdk/producer_monitor/store.ex):

| Callback | Responsibility |
| --- | --- |
| `load_session/0` | Return the committed `Store.Session` |
| `load_producer_progress/0` | Return a map of producer IDs to `Store.ProducerProgress` |
| `commit_session_change/1` | Atomically store the tokens and advance the generation |
| `advance_checkpoint/2` | Advance one producer's checkpoint monotonically |
| `require_recovery/1` | Clear one producer's synchronized generation |
| `mark_synchronized/2` | Record one producer's synchronized generation |

Mutation callbacks return the updated record directly, after the write completes.
Preserve unrelated fields and make each mutation atomic. Backend failures must
remain visible; do not return empty records or report success when a write fails.

Each logical store supports exactly one writer: its `ProducerMonitor`. If SDK
instances share a database, isolate each monitor's records in a stable namespace.
The store does not coordinate ownership between instances.

If your backend is already supervised by the application, omit the optional store
`child_spec/1` and start its infrastructure before the SDK:

```elixir
children = [
  MyApp.Repo,
  UOF.SDK
]
```

If the store owns a process, implement `child_spec/1`; the SDK starts it before
`ProducerMonitor`. The [ETS backend](lib/uof/sdk/producer_monitor/store/ets.ex)
provides a small reference implementation of the callback semantics.

See [ProducerMonitor.Store](docs/architecture.md#producermonitorstore) in the
architecture guide for record semantics, callback lifecycle, durability,
namespace isolation, and implementation validation.

## Architecture

The SDK supervises a producer monitor, a shared transport client, and separate
Broadway pipelines for system and content messages. Direct AMQP uses one connection
with a channel per pipeline; Pulsar uses Failover for system processing and Key-Shared
for distributed content delivery.

See the [architecture guide](docs/architecture.md) for process ownership, message flow,
startup, recovery, persistence, and restart behaviour.

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

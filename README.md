# Betradar UOF SDK

> [!IMPORTANT]
> This is an **unofficial Elixir SDK** for Betradar's Unified Odds Feed (UOF).
> Betradar offers official Java and .NET SDKs. You can read more about these
> [here](https://sdk.sportradar.com).

An Elixir SDK for Betradar's [Unified Odds Feed](https://docs.betradar.com/) (UOF).
Connects to the AMQP feed, decodes XML messages into structs, delivers them to a
handler you implement, and keeps each producer in sync via automatic recovery.
Built on [Broadway](https://hexdocs.pm/broadway); depends on
[`uof_api`](https://hex.pm/packages/uof_api) and
[`uof_schemas`](https://hex.pm/packages/uof_schemas).

## Architecture

`UOF.SDK` is a library supervisor that starts three components in order:

```
UOF.SDK
├── CheckpointStore   – last stable alive timestamp per producer
├── ProducerMonitor   – producer state, health monitoring, and recovery
├── SystemPipeline    – AMQP consumer for alive and snapshot_complete
└── ContentPipeline   – AMQP consumer for event content
```

Messages flow in one direction: the pipelines receive raw AMQP messages, decode
the XML payload, and call your `MessageHandler`. `SystemPipeline` owns system
traffic (`alive`, `snapshot_complete`) and notifies `ProducerMonitor` for
producer lifecycle, recovery correlation, and checkpoint advancement.
`ContentPipeline` owns event content and reports content-queue timestamps for
lag detection. It also consumes session-scoped `alive` messages only as lag
freshness markers for quiet producers. Producer state lives entirely in
`ProducerMonitor`'s GenServer state; `UOF.SDK.producers/0` goes through a
`GenServer.call`.

## Configuration

> [!NOTE]
> Configure one transport. The SDK derives the separate Broadway producers it
> needs for content and system messages.

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

# The HTTP client (recovery + producer descriptions)
config :uof_api,
  base_url: "https://stgapi.betradar.com/v1",
  auth_token: System.get_env("UOF_ACCESS_TOKEN")
```

For AMQP, `transport: {:amqp, connection: [...]}` is passed verbatim to
[`BroadwayRabbitMQ.Producer`](https://hexdocs.pm/broadway_rabbitmq/BroadwayRabbitMQ.Producer.html)
for both pipelines, with SDK-owned bindings for content and system traffic.
Known Betradar AMQP hosts: `mq.betradar.com` (production),
`stgmq.betradar.com` (integration), `replaymq.betradar.com` (replay).
Applications using AMQP must include `{:broadway_rabbitmq, "~> 0.8"}` in their
own dependencies.

For Pulsar, configure one topic and base subscription. The SDK derives the
content subscription as Key-Shared and the system subscription as Failover:

```elixir
config :uof_sdk,
  handler: MyApp.FeedHandler,
  node_id: 1,
  transport: {:pulsar,
    host: "pulsar://localhost:6650",
    topic: "uof-feed",
    subscription: "uof-sdk",
    routing_key_metadata_key: :pulsar_key,
    connection_token_metadata_key: :pulsar_connection
  }
```

Applications using Pulsar must include `{:off_broadway_pulsar, "~> 1.4"}` in
their own dependencies.

| Option | Default | Notes |
|--------|---------|-------|
| `:handler` | — (required) | Your `UOF.SDK.MessageHandler` module |
| `:transport` | `:amqp` | `{:amqp, opts}` or `{:pulsar, opts}` |
| `:node_id` | `nil` | Scopes AMQP bindings and recovery `snapshot_complete` per client |
| `:checkpoint_store` | `UOF.SDK.CheckpointStore.ETS` | Recovery checkpoint persistence |
| `:concurrency` | `10` | Broadway processor concurrency per feed session |
| `:inactivity_seconds` | `20` | Alive-gap threshold before a producer is marked down and recovered |
| `:max_processing_delay_seconds` | `20` | Consumer-lag threshold before a producer is marked `delayed?` (no recovery) |
| `:min_interval_between_recoveries` | `30` | Recovery cooldown (seconds) |
| `:max_recovery_time` | `3600` | Stall deadline before reissuing recovery (seconds) |
| `:recovery_overlap_seconds` | `300` | Seconds subtracted from the stored checkpoint when requesting incremental recovery |

> Recovery throttling defaults follow the official SDK guidance. `:recovery_overlap_seconds` is specific to this SDK and should be tuned for your deployment.

For Pulsar transports, `:routing_key_metadata_key` must point at the original
UOF routing key in the Broadway message metadata. `:connection_token_metadata_key`
can point at a per-connection token used for reconnect detection on both
pipelines.

## Usage

> [!NOTE]
> `UOF.SDK.LogHandler` logs every message and producer-status change. Swap it in
> for `handler` in your config for a first connection without writing your own handler.

Implement a handler — `use UOF.SDK.MessageHandler` gives no-op defaults for every
callback, so override only what you need:

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
    # producer.down?, producer.delayed?, producer.reason
    :ok
  end
end
```

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

Every callback except `handle_producer_status/1` receives the decoded feed struct
and a `UOF.SDK.Context` (`producer_id`, `event_urn`, `routing_key`, `message_type`).

Raw `alive` messages are consumed internally for recovery, checkpointing, and
producer health. Applications should use `handle_producer_status/1` instead of
reacting to heartbeat traffic directly.

> Return quickly from callbacks — a slow handler delays that event's later messages and may trigger a "slow processing" down. Offload heavy work asynchronously.

Add the SDK to your supervision tree — it's a library supervisor, so it connects
only when your app starts it:

```elixir
children = [
  MyApp.Repo,
  UOF.SDK
]

Supervisor.start_link(children, strategy: :one_for_one)
```

## Producer health and recovery

> [!NOTE]
> The default `UOF.SDK.CheckpointStore.ETS` is in-memory: checkpoints are lost
> on VM restart and a full recovery is issued on next start. This is fine for
> development and low-volume producers; use a persistent store in production.

Producer state is available synchronously and via the `handle_producer_status/1`
callback (same `UOF.SDK.Producer` struct):

```elixir
UOF.SDK.producers()
#=> [%UOF.SDK.Producer{id: 1, product: "liveodds", down?: false, ...}, ...]

UOF.SDK.producer(1)
#=> {:ok, %UOF.SDK.Producer{...}}
```

A producer is reported **down** (don't accept bets) for one of two independent
reasons:

- **Feed delivery** (`:alive_interval_violation`, `:connection_down`, `:other`)
  — the SDK issues a recovery and returns the producer up via
  `:returned_from_inactivity` / `:first_recovery_completed`.
- **Slow local processing** (`:processing_queue_delay_violation`) — no recovery;
  the remote producer is healthy. It returns up via
  `:processing_queue_delay_stabilized` once your handler catches up.

The UOF protocol requires every producer to be *recovered* before its markets are
safe to act on. A gap in the message stream — on first connect, reconnect, or
alive heartbeat timeout — leaves local state out of sync with the remote producer.
When a gap is detected, the SDK:

1. Reads `CheckpointStore` for the last stable alive timestamp for that producer.
2. Issues a `UOF.API.Recovery.recover/2` call with a unique `request_id` —
   incremental (from the checkpoint timestamp) if one exists, full otherwise.
3. Betradar replays the missing messages back over the same AMQP feed.
4. When `snapshot_complete` arrives with the matching `request_id`, the producer
   is marked up and `handle_producer_status/1` fires.

`snapshot_complete` is handled on the system pipeline by design. It means the
feed has finished publishing a recovery replay, not that this SDK instance has
finished executing all handler callbacks for replayed content. Local backlog is
handled separately by the content lag monitor: if processed content-queue
timestamps fall behind by more than `:max_processing_delay_seconds`, the
producer is marked `delayed?` / down until processing catches up. Event messages
and content-session `alive` messages both advance this lag timestamp; system
`alive` messages do not. Recovery overlap and idempotent handlers cover the
remaining crash/restart window.

A stall guard (`:max_recovery_time`) reissues the request if no
`snapshot_complete` arrives within the deadline, preserving the original
timestamp so no messages are skipped on retry.

**Checkpoints** are owned by `ProducerMonitor` and are advanced from subscribed
system `alive` heartbeats after the producer is already in sync. Content
messages and content-session `alive` messages do not write checkpoints directly.
On recovery, the SDK subtracts
`:recovery_overlap_seconds` from the stored checkpoint before requesting
incremental recovery. This intentionally replays a bounded amount of data to
cover concurrent processing and distributed-consumer skew; handlers should be
idempotent and tolerate duplicates. Without a checkpoint, a full recovery is
issued. To persist checkpoints across VM restarts, implement the
`UOF.SDK.CheckpointStore` behaviour (`get/1`, `put/2`, `delete/1`) and configure
it:

```elixir
config :uof_sdk, checkpoint_store: MyApp.PostgresCheckpointStore
```

## License

MIT

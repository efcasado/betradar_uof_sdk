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
├── CheckpointStore   – last-seen feed timestamp per producer
├── ProducerMonitor   – producer state, health monitoring, and recovery
└── Pipeline          – AMQP consumer; decodes and dispatches messages
```

Messages flow in one direction: the `Pipeline` receives raw AMQP messages, decodes
the XML payload, and calls your `MessageHandler`. As a side-effect it notifies
`ProducerMonitor` of each `alive` heartbeat, content-message timestamp, and
`snapshot_complete`. Producer state lives entirely in `ProducerMonitor`'s GenServer
state; `UOF.SDK.producers/0` goes through a `GenServer.call`.

## Configuration

> [!NOTE]
> The SDK uses [`BroadwayRabbitMQ.Producer`](https://hexdocs.pm/broadway_rabbitmq/BroadwayRabbitMQ.Producer.html) by default. Set `:producer` to use a
> custom Broadway producer instead (e.g. Pulsar), and omit `:connection`. With a
> custom producer, two settings matter: `:routing_key_metadata_key` (the metadata
> field carrying the UOF routing key — partitioning, dispatch, and lifecycle all
> derive from it) and `:connection_token_metadata_key` (a per-connection token for
> reconnect detection; without it, micro-disconnections may go unnoticed and leave
> a message gap silently unfilled).

```elixir
config :uof_sdk,
  handler: MyApp.FeedHandler,
  node_id: 1,
  connection: [
    host: "stgmq.betradar.com",
    username: System.get_env("UOF_ACCESS_TOKEN"),
    password: "",
    virtual_host: "/unifiedfeed/12345",
    ssl_options: []
  ]

# The HTTP client (recovery + producer descriptions)
config :uof_api,
  base_url: "https://stgapi.betradar.com/v1",
  auth_token: System.get_env("UOF_ACCESS_TOKEN")
```

`:connection` is passed verbatim to `BroadwayRabbitMQ.Producer` — no fields are
derived or defaulted. Known Betradar AMQP hosts: `mq.betradar.com` (production),
`stgmq.betradar.com` (integration), `replaymq.betradar.com` (replay).

| Option | Default | Notes |
|--------|---------|-------|
| `:handler` | — (required) | Your `UOF.SDK.MessageHandler` module |
| `:connection` | `[]` | `BroadwayRabbitMQ.Producer` connection options (ignored when `:producer` is set) |
| `:node_id` | `nil` | Scopes AMQP bindings and recovery `snapshot_complete` per client |
| `:producer` | `nil` | Custom Broadway producer spec; overrides `:connection` |
| `:routing_key_metadata_key` | `:routing_key` | Metadata field carrying the UOF routing key (custom producers only) |
| `:connection_token_metadata_key` | `nil` | Metadata field carrying a per-connection token for reconnect detection (custom producers only) |
| `:checkpoint_store` | `UOF.SDK.CheckpointStore.ETS` | Recovery checkpoint persistence |
| `:inactivity_seconds` | `20` | Alive-gap threshold before a producer is marked down and recovered |
| `:max_processing_delay_seconds` | `20` | Consumer-lag threshold before a producer is marked `delayed?` (no recovery) |
| `:min_interval_between_recoveries` | `30` | Recovery cooldown (seconds) |
| `:max_recovery_time` | `3600` | Stall deadline before reissuing recovery (seconds) |

> Recovery defaults mirror the official SDK. Change with care — see Betradar's recovery docs on throttling.

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

| Callback | When it fires |
|----------|---------------|
| `handle_odds_change/2` | Odds updated for a sport event |
| `handle_bet_settlement/2` | Markets settled after an event |
| `handle_bet_stop/2` | Betting suspended on a market |
| `handle_bet_cancel/2` | Bets cancelled on a market |
| `handle_rollback_bet_cancel/2` | Cancellation rolled back |
| `handle_rollback_bet_settlement/2` | Settlement rolled back |
| `handle_fixture_change/2` | Fixture metadata changed |
| `handle_alive/2` | Heartbeat from the producer (~10 s cadence) |
| `handle_producer_status/1` | Producer health changed (up / down / recovering) |

Every callback except `handle_producer_status/1` receives the decoded feed struct
and a `UOF.SDK.Context` (`producer_id`, `event_urn`, `routing_key`, `message_type`).

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

1. Reads `CheckpointStore` for the last processed timestamp for that producer.
2. Issues a `UOF.API.Recovery.recover/2` call with a unique `request_id` —
   incremental (from the checkpoint timestamp) if one exists, full otherwise.
3. Betradar replays the missing messages back over the same AMQP feed.
4. When `snapshot_complete` arrives with the matching `request_id`, the producer
   is marked up and `handle_producer_status/1` fires.

A stall guard (`:max_recovery_time`) reissues the request if no
`snapshot_complete` arrives within the deadline, preserving the original
timestamp so no messages are skipped on retry.

**Checkpoints** are the timestamp of the last processed message per producer.
With a valid checkpoint, recovery is incremental — only missed messages are
replayed. Without one, a full recovery is issued. To persist checkpoints across
VM restarts, implement the `UOF.SDK.CheckpointStore` behaviour (`get/1`, `put/2`,
`delete/1`) and configure it:

```elixir
config :uof_sdk, checkpoint_store: MyApp.PostgresCheckpointStore
```

## License

MIT

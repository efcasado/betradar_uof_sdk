# Betradar UOF SDK

An Elixir SDK for Betradar's [Unified Odds Feed](https://docs.betradar.com/) (UOF).
It connects to the UOF AMQP feed, decodes the XML messages into structs, delivers
them to a handler you implement, and keeps each producer in sync with the feed by
orchestrating odds recovery automatically.

It is built on [Broadway](https://hexdocs.pm/broadway) (backpressure, per-event
ordering, fault tolerance) and depends on
[`uof_api`](https://hex.pm/packages/uof_api) for the HTTP calls (recovery,
producer descriptions, `whoami`), and [`uof_schemas`](https://hex.pm/packages/uof_schemas) for the decoding of feed
messages.

## Features

- **Turnkey feed consumption** — connects, subscribes, decodes, dispatches.
- **Per-event ordering with concurrency** — messages are partitioned by
  sport-event so a given event is always processed in order, while different
  events run in parallel.
- **Automatic recovery** — producers start down and are recovered on connect,
  on reconnect (the connection gap is detected and filled), when alives stop,
  and on `subscribed=0`.
- **Producer health** — a two-axis model (feed-delivery vs. slow local
  processing) exposed both as a callback and via `UOF.SDK.producers/0`.
- **Pluggable recovery checkpoints** — ETS by default; bring your own
  (PostgreSQL, Redis, …) to resume recovery across restarts.

## Installation

```elixir
def deps do
  [
    {:betradar_uof_sdk, "~> 0.1"}
  ]
end
```

## Configuration

```elixir
# The SDK
config :betradar_uof_sdk,
  handler: MyApp.FeedHandler,
  access_token: System.get_env("UOF_ACCESS_TOKEN"),
  host: "stgmq.betradar.com",         # the AMQP endpoint, explicit
  node_id: 1,                         # isolate this client on a shared account
  checkpoint_store: UOF.SDK.CheckpointStore.ETS

# The HTTP client it depends on (recovery + descriptions)
config :uof_api,
  base_url: "https://stgapi.betradar.com/v1",
  auth_token: System.get_env("UOF_ACCESS_TOKEN")
```

The AMQP endpoint is explicit — `:host` is required, `:port` defaults to `5671`
and `:ssl` to `true`. Known Betradar hosts: `mq.betradar.com` (production),
`stgmq.betradar.com` (integration), `replaymq.betradar.com` (replay). Raw
`BroadwayRabbitMQ` connection options (e.g. `ssl_options`) go under `:amqp`. The
virtual host is derived from `UOF.API.Users.whoami/0` at startup; set
`:virtual_host` to override.

| Option | Default | Notes |
|--------|---------|-------|
| `:handler` | — (required) | Your `UOF.SDK.MessageHandler` module |
| `:access_token` | — (required) | UOF access token (AMQP username) |
| `:host` | — (required) | AMQP endpoint host |
| `:port` | `5671` | AMQP port |
| `:ssl` | `true` | Enable TLS |
| `:node_id` | `nil` | Scopes recovery/`snapshot_complete` per client |
| `:checkpoint_store` | `UOF.SDK.CheckpointStore.ETS` | Recovery checkpoint persistence |
| `:inactivity_seconds` | `20` | Down threshold for the two health axes |
| `:min_interval_between_recoveries` | `30` | Recovery cooldown |
| `:max_recovery_time` | `3600` | Stall deadline before reissuing recovery |
| `:amqp` | `[]` | Raw `connection` options (`ssl_options`, `heartbeat`, …), merged in last |

> Defaults for the recovery knobs mirror the official SDK and are
> throttling-safe; change them only after reading Betradar's recovery docs.

## Usage

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

Callbacks: `handle_odds_change/2`, `handle_bet_settlement/2`, `handle_bet_stop/2`,
`handle_bet_cancel/2`, `handle_rollback_bet_cancel/2`,
`handle_rollback_bet_settlement/2`, `handle_fixture_change/2`, `handle_alive/2`,
and `handle_producer_status/1`.

> Return from callbacks quickly. The pipeline processes per event in order, so a
> slow handler delays that event's later messages and can mark the producer
> down for "slow processing". Offload heavy work (DB writes, HTTP) asynchronously.

Add the SDK to your supervision tree — it's a library supervisor, so it connects
only when your app starts it:

```elixir
children = [
  MyApp.Repo,
  UOF.SDK
]

Supervisor.start_link(children, strategy: :one_for_one)
```

## Producer health

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

## Custom checkpoint store

The default ETS store loses checkpoints on VM restart (which simply falls back to
a full recovery). To resume incremental recovery across restarts, implement the
`UOF.SDK.CheckpointStore` behaviour (`get/1`, `put/2`, `delete/1`) and point the
config at it:

```elixir
config :betradar_uof_sdk, checkpoint_store: MyApp.PostgresCheckpointStore
```

## Smoke testing

`UOF.SDK.LogHandler` logs every message and producer-status change — handy for a
first connection:

```elixir
UOF.SDK.start_link(
  handler: UOF.SDK.LogHandler,
  access_token: System.fetch_env!("UOF_ACCESS_TOKEN"),
  host: "stgmq.betradar.com",
  node_id: 1
)
```

## License

MIT

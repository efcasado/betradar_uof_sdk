# Betradar UOF SDK

> [!IMPORTANT]
> This is an **unofficial Elixir SDK** for Betradar's Unified Odds Feed (UOF).
> Betradar offers official Java and .NET SDKs at <https://sdk.sportradar.com>.

An Elixir SDK for Betradar's [Unified Odds Feed](https://docs.betradar.com/)
(UOF). It consumes the feed from an Apache Pulsar topic, decodes XML messages into
structs, sends those messages to a handler you implement, and keeps each producer in
sync with automatic recovery.

The SDK is built on [Broadway](https://hexdocs.pm/broadway) and
[`off_broadway_pulsar`](https://hex.pm/packages/off_broadway_pulsar), and depends on
[`uof_api`](https://hex.pm/packages/uof_api) and
[`uof_schemas`](https://hex.pm/packages/uof_schemas).

The SDK handles:

- Feed consumption from Pulsar, scaled out across instances.
- XML decoding into Elixir structs.
- Message delivery to your application callbacks.
- Producer health tracking.
- Recovery after startup, upstream reconnects, or feed gaps.

Your application handles:

- Business logic for odds changes, settlements, bet stops, and other feed
  messages.
- Persistence of the data you care about.
- Idempotency for replayed or duplicated messages.
- [Persistent checkpoint storage](docs/deployment.md#monitor-store) in
  production.

## Guides

- [Architecture](docs/architecture.md): components, feed topology, message processing,
  persistence, and design invariants.
- [Deployment](docs/deployment.md): the source connector, topic, subscriptions, and
  scaling.
- [Recovery](docs/recovery.md): producer status, automatic recovery, manual and full
  recoveries, and what to monitor.

## Installation

Add the SDK to `mix.exs`, then run `mix deps.get`. The SDK requires Elixir 1.20+.

```elixir
def deps do
  [
    {:uof_sdk, "~> 0.1.0"}
  ]
end
```

## Quick start

The SDK consumes the feed from a Pulsar topic that a RabbitMQ source connector fills from
Betradar. This example assumes that is in place, publishing the integration feed to the
`uof-feed` topic; the [deployment guide](docs/deployment.md) describes how to set it up. You
need a UOF access token for the recovery API.

Set `UOF_ACCESS_TOKEN` in your environment. In `config/runtime.exs`, configure the
Pulsar topic and the recovery API for the same environment as the connector:

```elixir
import Config

config :uof_sdk,
  handler: UOF.SDK.LogHandler,
  node_id: 1,
  pulsar: [
    host: "pulsar://localhost:6650",
    topic: "uof-feed",
    subscription: "uof-sdk"
  ]

config :uof_api,
  base_url: "https://stgapi.betradar.com/v1",
  auth_token: System.fetch_env!("UOF_ACCESS_TOKEN")
```

The SDK needs API access for producer descriptions and recovery. See
[configuration options](#configuration), and the deployment guide for
[`node_id` and scaling](docs/deployment.md#sdk-configuration).

Add `UOF.SDK` to your application's supervision tree:

```elixir
children = [
  UOF.SDK
]

Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
```

For an interactive smoke test instead, call `UOF.SDK.start_link([])` from
`iex -S mix`. Start the SDK only once per VM.

The SDK connects, synchronizes producers, and logs feed messages through
`UOF.SDK.LogHandler`. Startup does not mean recovery has completed. Inspect
producer health with:

```elixir
UOF.SDK.producers()
UOF.SDK.producer(1)
```

To handle messages yourself, replace the log handler with your own module.
`use UOF.SDK.MessageHandler` provides no-op defaults, so override only the
callbacks you need:

```elixir
defmodule MyApp.FeedHandler do
  use UOF.SDK.MessageHandler

  require Logger

  @impl true
  def handle_odds_change(odds_change, ctx) do
    Logger.debug("Odds changed for #{ctx.event_urn}: #{inspect(odds_change)}")
    :ok
  end

  @impl true
  def handle_producer_status(producer) do
    Logger.info("Producer #{producer.id}: #{producer.status}")
    :ok
  end
end
```

Set `handler: MyApp.FeedHandler` in the SDK configuration. Content callbacks
receive the decoded feed struct and a `UOF.SDK.Context` with producer, message,
routing, and event identity. Keep callbacks short, return `:ok`, and make effects
idempotent. If you offload work, make the handoff durable before returning.

The [architecture guide](docs/architecture.md) covers the details beyond first
connection.

## Configuration

Configure `:uof_sdk` through application environment, as shown in the
[quick start](#quick-start). `UOF.SDK.start_link/1` also accepts keyword
overrides. The SDK derives its system and content consumers from `:pulsar`.
The `:uof_api` access token and base URL must match the feed environment because startup
loads producer descriptions and recovery uses the HTTP API.

| Option | Default | Purpose |
| --- | --- | --- |
| `:handler` | Required | Module implementing `UOF.SDK.MessageHandler` |
| `:pulsar` | Required | `:host`, `:topic`, and base `:subscription`; `:conn_opts` for the client; other options go to `OffBroadway.Pulsar.Producer` |
| `:node_id` | `nil` | Positive ID scopes recovery replays and `snapshot_complete`; shared by every instance on one subscription, distinct across subscriptions using the same access token |
| `:monitor_store` | `UOF.SDK.ProducerMonitor.Store.ETS` | `module` or `{module, opts}` persisting restart state; see [implementing a store](docs/architecture.md#implementing-a-store) |
| `:concurrency` | `10` | Number of content processors in each SDK instance |
| `:inactivity_seconds` | `20` | Heartbeat timeout and session-readiness waiting interval |
| `:min_interval_between_recoveries` | `30` | Per-producer recovery-request cooldown and retry delay |
| `:max_recovery_time` | `600` | Wait for recovery completion before reissuing a stalled request; a reissue is logged as a warning |
| `:producers` | `[]` | Fallback producer descriptions (`[id: 1, product: "liveodds", recovery_window_minutes: 4320]`), used only if the UOF API is unavailable at startup and no previous run stored any |

Public durations are in seconds. Recovery throttling defaults follow the official SDK
guidance.
See [time controls](docs/recovery.md#time-controls) for the precise semantics.

## Handler reference

Implement `UOF.SDK.MessageHandler` with `use UOF.SDK.MessageHandler`, then set the module
as `:handler`. The [quick start](#quick-start) shows a minimal handler;
all callbacks have overridable no-op defaults. Content callbacks receive a decoded
`UOF.Schemas.Feed.*` struct and a `UOF.SDK.Context` containing `producer_id`, `message_type`,
`routing_key`, and `event_urn`.

| Callback | Feed message or lifecycle signal |
| --- | --- |
| `handle_odds_change/2` | [Odds Change](https://docs.sportradar.com/uof/data-and-features/messages/event/odds-change) |
| `handle_bet_settlement/2` | [Bet Settlement](https://docs.sportradar.com/uof/data-and-features/messages/event/bet-settlement) |
| `handle_bet_stop/2` | [Bet Stop](https://docs.sportradar.com/uof/data-and-features/messages/event/bet-stop) |
| `handle_bet_cancel/2` | [Bet Cancel](https://docs.sportradar.com/uof/data-and-features/messages/event/bet-cancel) |
| `handle_rollback_bet_cancel/2` | [Rollback Bet Cancel](https://docs.sportradar.com/uof/data-and-features/messages/event/rollback-bet-cancel) |
| `handle_rollback_bet_settlement/2` | [Rollback Bet Settlements](https://docs.sportradar.com/uof/data-and-features/messages/event/rollback-bet-settlements) |
| `handle_fixture_change/2` | [Fixture Change](https://docs.sportradar.com/uof/data-and-features/messages/event/fixture-change) |
| `handle_producer_status/1` | Lifecycle status transition; receives the same producer struct as `UOF.SDK.producer/1` |

Raw `alive` and `snapshot_complete` messages are internal. Status callbacks run only when
the lifecycle status changes; timestamp and checkpoint updates do not invoke them.

See [message processing](docs/architecture.md#message-processing) for ordering, delivery,
and failure semantics.

## Testing

Use `make compile` and `make test-unit` for development checks. The Docker-backed
integration test builds the RabbitMQ source connector, starts the Compose environment,
verifies synthetic UOF events end to end, and cleans up afterward:

```bash
make test-integration
```

The connector repository, revision, and checkout directory can be overridden with
`RABBITMQ_SOURCE_REPO`, `RABBITMQ_SOURCE_REF`, and `RABBITMQ_SOURCE_DIR`.
[`Makefile`](Makefile) pins a connector revision with the required message-key rules
and routing-key property support. The built connector is cached per revision, locally
under `tmp/` and in CI, so changing `RABBITMQ_SOURCE_REF` triggers a rebuild. Ordinary
`mix test` excludes the Docker-backed test.

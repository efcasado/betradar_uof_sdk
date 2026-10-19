# Betradar UOF SDK

> [!IMPORTANT]
> This is an **unofficial Elixir SDK** for Betradar's Unified Odds Feed (UOF).
> Betradar offers official Java and .NET SDKs at <https://sdk.sportradar.com>.

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
- [Persistent checkpoint storage](docs/architecture.md#producermonitorstore) in
  production.

## Installation

Add the SDK and one transport adapter to `mix.exs`, then run `mix deps.get`. The
SDK requires Elixir 1.20+.

```elixir
def deps do
  [
    {:uof_sdk, "~> 0.1.0"},
    {:broadway_rabbitmq, "~> 0.8.2"}
  ]
end
```

For Pulsar, replace `broadway_rabbitmq` with `{:off_broadway_pulsar, "~> 2.0"}`.
Only the adapter for your chosen transport is required.

## Quick start

This example connects directly to Betradar over AMQP. You need a UOF access token
and your assigned virtual host. For distributed consumption, see
[Pulsar setup](docs/architecture.md#pulsar).

Set `UOF_ACCESS_TOKEN` in your environment. In `config/runtime.exs`, configure the
feed and recovery API for the same environment. Replace `/unifiedfeed/12345` with
your assigned virtual host:

```elixir
import Config

config :uof_sdk,
  handler: UOF.SDK.LogHandler,
  node_id: 1,
  transport: {:amqp,
    connection: [
      host: "stgmq.betradar.com",
      username: System.fetch_env!("UOF_ACCESS_TOKEN"),
      password: "",
      virtual_host: "/unifiedfeed/12345",
      ssl_options: []
    ]
  }

config :uof_api,
  base_url: "https://stgapi.betradar.com/v1",
  auth_token: System.fetch_env!("UOF_ACCESS_TOKEN")
```

These are integration-environment endpoints. See the guide for
[AMQP hosts](docs/architecture.md#direct-amqp) and
[configuration options](docs/architecture.md#configuration).

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

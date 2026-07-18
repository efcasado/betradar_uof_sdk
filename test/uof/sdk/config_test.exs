defmodule UOF.SDK.ConfigTest do
  use ExUnit.Case, async: true

  alias OffBroadway.Pulsar.Producer
  alias UOF.SDK.Config

  test "builds AMQP producer specs from the transport connection" do
    conn = [host: "stgmq.betradar.com", username: "tok", password: "", ssl_options: []]
    config = Config.load(handler: MyApp.Handler, transport: {:amqp, connection: conn})

    assert {BroadwayRabbitMQ.Producer, content_opts} = config.content_producer
    assert {BroadwayRabbitMQ.Producer, system_opts} = config.system_producer
    assert content_opts[:connection] == conn
    assert system_opts[:connection] == conn
    assert :consumer_tag in content_opts[:metadata]
    assert :consumer_tag in system_opts[:metadata]
    assert config.metadata_adapter == :amqp
    assert config.routing_key_metadata_key == :routing_key
    assert config.connection_token_metadata_key == nil
    assert config.ownership == :always_active
  end

  test "defaults to AMQP transport with empty connection and ETS snapshot store" do
    config = Config.load(handler: MyApp.Handler)

    assert config.transport == :amqp
    assert {BroadwayRabbitMQ.Producer, content_opts} = config.content_producer
    assert content_opts[:connection] == []
    assert config.monitor_store == UOF.SDK.ProducerMonitor.Store.ETS
  end

  test "scopes AMQP bindings by node_id" do
    config = Config.load(handler: MyApp.Handler, node_id: 42)

    assert {BroadwayRabbitMQ.Producer, content_opts} = config.content_producer
    assert {BroadwayRabbitMQ.Producer, system_opts} = config.system_producer

    assert {"unifiedfeed", routing_key: "*.*.*.odds_change.*.*.*.42.#"} in content_opts[:bindings]
    assert {"unifiedfeed", routing_key: "-.-.-.snapshot_complete.*.*.*.42.#"} in system_opts[:bindings]
  end

  test "accepts a custom snapshot store" do
    config = Config.load(handler: MyApp.Handler, monitor_store: MyApp.PgStore)
    assert config.monitor_store == MyApp.PgStore
  end

  test "builds Pulsar producer specs from one topic and subscription" do
    config =
      Config.load(
        handler: MyApp.Handler,
        transport:
          {:pulsar,
           host: "pulsar://localhost:6650",
           topic: "uof-feed",
           subscription: "uof-sdk",
           consumer_opts: [initial_position: :earliest]}
      )

    assert {Producer, content_opts} = config.content_producer
    assert {Producer, system_opts} = config.system_producer

    assert content_opts[:host] == "pulsar://localhost:6650"
    assert content_opts[:topic] == "uof-feed"
    assert content_opts[:subscription] == "uof-sdk-content"
    assert content_opts[:consumer_opts][:initial_position] == :earliest
    assert content_opts[:consumer_opts][:subscription_type] == :Key_Shared

    assert system_opts[:topic] == "uof-feed"
    assert system_opts[:subscription] == "uof-sdk-system"
    assert system_opts[:consumer_opts][:initial_position] == :earliest
    assert system_opts[:consumer_opts][:subscription_type] == :Failover

    # Failover ownership reports gate the control plane; the Key_Shared
    # content subscription never emits them.
    assert system_opts[:active_state_callback] ==
             {UOF.SDK.ProducerMonitor, :active_state_change, []}

    refute Keyword.has_key?(content_opts, :active_state_callback)

    assert config.metadata_adapter == :pulsar_rabbitmq_source
    assert config.routing_key_metadata_key == :routing_key
    assert config.connection_token_metadata_key == nil
    assert config.ownership == {:failover, :passive}
  end

  test "requires a Pulsar subscription" do
    assert_raise KeyError, fn ->
      Config.load(handler: MyApp.Handler, transport: {:pulsar, topic: "uof-feed"})
    end
  end

  test "requires a Pulsar topic" do
    assert_raise KeyError, fn ->
      Config.load(handler: MyApp.Handler, transport: {:pulsar, subscription: "uof-sdk"})
    end
  end

  test "rejects unsupported transports" do
    assert_raise ArgumentError, ~r/unsupported UOF.SDK transport/, fn ->
      Config.load(handler: MyApp.Handler, transport: {:kafka, []})
    end
  end

  test "stores node_id when provided" do
    config = Config.load(handler: MyApp.Handler, node_id: 42)
    assert config.node_id == 42
  end

  test "defaults the health thresholds and overrides max_processing_delay_seconds" do
    defaults = Config.load(handler: MyApp.Handler)
    assert defaults.inactivity_seconds == 20
    assert defaults.max_processing_delay_seconds == 20
    assert defaults.recovery_overlap_seconds == 300

    tuned = Config.load(handler: MyApp.Handler, max_processing_delay_seconds: 45)
    assert tuned.max_processing_delay_seconds == 45
    assert tuned.inactivity_seconds == 20
  end

  test "accepts a recovery overlap override" do
    config = Config.load(handler: MyApp.Handler, recovery_overlap_seconds: 900)
    assert config.recovery_overlap_seconds == 900
  end

  test "defaults concurrency to 10 and accepts an override" do
    assert Config.load(handler: MyApp.Handler).concurrency == 10
    assert Config.load(handler: MyApp.Handler, concurrency: 50).concurrency == 50
  end

  test "raises on missing :handler" do
    assert_raise ArgumentError, ~r/:handler/, fn -> Config.load([]) end
  end
end

defmodule UOF.SDK.ConfigTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.Config

  test "passes :connection through verbatim" do
    conn = [host: "stgmq.betradar.com", username: "tok", password: "", ssl_options: []]
    config = Config.load(handler: MyApp.Handler, connection: conn)

    assert config.connection == conn
  end

  test "defaults to empty connection and ETS checkpoint store" do
    config = Config.load(handler: MyApp.Handler)

    assert config.connection == []
    assert config.checkpoint_store == UOF.SDK.CheckpointStore.ETS
  end

  test "accepts a custom checkpoint store" do
    config = Config.load(handler: MyApp.Handler, checkpoint_store: MyApp.PgStore)
    assert config.checkpoint_store == MyApp.PgStore
  end

  test "requires a system producer when the content producer is custom" do
    assert_raise ArgumentError, ~r/:system_producer is required/, fn ->
      Config.load(handler: MyApp.Handler, producer: {Broadway.DummyProducer, []})
    end
  end

  test "accepts custom content and system producers" do
    content_producer = {Broadway.DummyProducer, []}
    system_producer = {Broadway.DummyProducer, transformer: :system}

    config =
      Config.load(
        handler: MyApp.Handler,
        producer: content_producer,
        system_producer: system_producer
      )

    assert config.producer == content_producer
    assert config.system_producer == system_producer
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

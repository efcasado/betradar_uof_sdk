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

  test "stores node_id when provided" do
    config = Config.load(handler: MyApp.Handler, node_id: 42)
    assert config.node_id == 42
  end

  test "raises on missing :handler" do
    assert_raise ArgumentError, ~r/:handler/, fn -> Config.load([]) end
  end
end

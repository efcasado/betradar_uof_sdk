defmodule UOF.SDKTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.Config
  alias UOF.SDK.Pipeline

  test "child_specs produces a pipeline with the configured handler and connection" do
    conn = [host: "stgmq.betradar.com", username: "tok", password: "", ssl_options: []]
    config = Config.load(handler: MyApp.Handler, connection: conn)

    assert [{Pipeline, opts}] = UOF.SDK.child_specs(config)
    assert opts[:name] == Pipeline
    assert opts[:handler] == MyApp.Handler
    assert opts[:connection] == conn
  end

  test "child_specs passes node_id through to the pipeline" do
    config = Config.load(handler: MyApp.Handler, node_id: 7)
    [{Pipeline, opts}] = UOF.SDK.child_specs(config)
    assert opts[:node_id] == 7
  end

  test "child_specs passes concurrency through to the pipeline" do
    assert [{Pipeline, default}] = UOF.SDK.child_specs(Config.load(handler: MyApp.Handler))
    assert default[:concurrency] == 10

    config = Config.load(handler: MyApp.Handler, concurrency: 50)
    [{Pipeline, opts}] = UOF.SDK.child_specs(config)
    assert opts[:concurrency] == 50
  end

  test "child_specs passes a custom producer spec through unchanged" do
    config = Config.load(handler: MyApp.Handler, producer: {Broadway.DummyProducer, []})
    [{Pipeline, opts}] = UOF.SDK.child_specs(config)
    assert opts[:producer] == {Broadway.DummyProducer, []}
  end
end

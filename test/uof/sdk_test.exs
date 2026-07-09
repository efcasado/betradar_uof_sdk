defmodule UOF.SDKTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.Config
  alias UOF.SDK.Pipeline
  alias UOF.SDK.SystemPipeline

  test "child_specs produces system and content pipelines with the configured handler and connection" do
    conn = [host: "stgmq.betradar.com", username: "tok", password: "", ssl_options: []]
    config = Config.load(handler: MyApp.Handler, connection: conn)

    assert [{SystemPipeline, system_opts}, {Pipeline, content_opts}] = UOF.SDK.child_specs(config)
    assert system_opts[:name] == SystemPipeline
    assert system_opts[:handler] == MyApp.Handler
    assert system_opts[:connection] == conn
    assert content_opts[:name] == Pipeline
    assert content_opts[:handler] == MyApp.Handler
    assert content_opts[:connection] == conn
  end

  test "child_specs passes node_id through to both pipelines" do
    config = Config.load(handler: MyApp.Handler, node_id: 7)
    [{SystemPipeline, system_opts}, {Pipeline, content_opts}] = UOF.SDK.child_specs(config)
    assert system_opts[:node_id] == 7
    assert content_opts[:node_id] == 7
  end

  test "child_specs passes concurrency through to the content pipeline" do
    assert [{SystemPipeline, _system}, {Pipeline, default}] =
             UOF.SDK.child_specs(Config.load(handler: MyApp.Handler))

    assert default[:concurrency] == 10

    config = Config.load(handler: MyApp.Handler, concurrency: 50)
    [{SystemPipeline, _system}, {Pipeline, opts}] = UOF.SDK.child_specs(config)
    assert opts[:concurrency] == 50
  end

  test "child_specs passes custom producer specs through unchanged" do
    content_producer = {Broadway.DummyProducer, []}
    system_producer = {Broadway.DummyProducer, transformer: :system}

    config =
      Config.load(
        handler: MyApp.Handler,
        producer: content_producer,
        system_producer: system_producer
      )

    [{SystemPipeline, system_opts}, {Pipeline, content_opts}] = UOF.SDK.child_specs(config)
    assert system_opts[:producer] == system_producer
    assert content_opts[:producer] == content_producer
  end
end

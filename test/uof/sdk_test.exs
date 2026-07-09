defmodule UOF.SDKTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.Config
  alias UOF.SDK.ContentPipeline
  alias UOF.SDK.SystemPipeline

  test "child_specs passes normalized producer specs to system and content pipelines" do
    conn = [host: "stgmq.betradar.com", username: "tok", password: "", ssl_options: []]
    config = Config.load(handler: MyApp.Handler, transport: {:amqp, connection: conn})

    assert [{SystemPipeline, system_opts}, {ContentPipeline, content_opts}] = UOF.SDK.child_specs(config)
    assert system_opts[:name] == SystemPipeline
    assert {BroadwayRabbitMQ.Producer, system_producer_opts} = system_opts[:producer]
    assert system_producer_opts[:connection] == conn

    assert content_opts[:name] == ContentPipeline
    assert content_opts[:handler] == MyApp.Handler
    assert {BroadwayRabbitMQ.Producer, content_producer_opts} = content_opts[:producer]
    assert content_producer_opts[:connection] == conn
  end

  test "child_specs passes concurrency through to the content pipeline" do
    assert [{SystemPipeline, _system}, {ContentPipeline, default}] =
             UOF.SDK.child_specs(Config.load(handler: MyApp.Handler))

    assert default[:concurrency] == 10

    config = Config.load(handler: MyApp.Handler, concurrency: 50)
    [{SystemPipeline, _system}, {ContentPipeline, opts}] = UOF.SDK.child_specs(config)
    assert opts[:concurrency] == 50
  end
end

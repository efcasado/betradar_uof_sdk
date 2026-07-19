defmodule UOF.SDKTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.Config
  alias UOF.SDK.ContentPipeline
  alias UOF.SDK.ProducerMonitor.Store
  alias UOF.SDK.ProducerMonitor.Store.ProducerProgress
  alias UOF.SDK.ProducerMonitor.Store.Session
  alias UOF.SDK.SystemPipeline

  defmodule CallbackOnlyMonitorStore do
    @moduledoc false
    @behaviour Store

    @impl true
    def load_session, do: %Session{}

    @impl true
    def load_producer_progress, do: %{}

    @impl true
    def commit_session_change(tokens), do: %Session{tokens: tokens, generation: 1}

    @impl true
    def advance_checkpoint(_id, timestamp), do: %ProducerProgress{checkpoint: timestamp}

    @impl true
    def require_recovery(_id), do: %ProducerProgress{}

    @impl true
    def mark_synchronized(_id, generation), do: %ProducerProgress{synchronized_generation: generation}
  end

  defmodule StartableMonitorStore do
    @moduledoc false
    @behaviour Store

    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl GenServer
    def init(opts), do: {:ok, opts}

    @impl true
    def load_session, do: %Session{}

    @impl true
    def load_producer_progress, do: %{}

    @impl true
    def commit_session_change(tokens), do: %Session{tokens: tokens, generation: 1}

    @impl true
    def advance_checkpoint(_id, timestamp), do: %ProducerProgress{checkpoint: timestamp}

    @impl true
    def require_recovery(_id), do: %ProducerProgress{}

    @impl true
    def mark_synchronized(_id, generation), do: %ProducerProgress{synchronized_generation: generation}
  end

  test "monitor_store_child_specs omits callback-only stores" do
    assert UOF.SDK.monitor_store_child_specs(CallbackOnlyMonitorStore) == []
  end

  test "monitor_store_child_specs includes stores with child specs" do
    assert UOF.SDK.monitor_store_child_specs(StartableMonitorStore) == [StartableMonitorStore]
  end

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

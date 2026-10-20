defmodule UOF.SDKTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.AMQP.Connection
  alias UOF.SDK.Config
  alias UOF.SDK.ContentPipeline
  alias UOF.SDK.LogHandler
  alias UOF.SDK.ProducerMonitor.Store
  alias UOF.SDK.SystemPipeline

  defmodule CallbackOnlyMonitorStore do
    @moduledoc false
    @behaviour Store

    @impl true
    def load(_opts), do: nil

    @impl true
    def save(_snapshot, _opts), do: :ok
  end

  defmodule StartableMonitorStore do
    @moduledoc false
    @behaviour Store

    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl GenServer
    def init(opts), do: {:ok, opts}

    @impl true
    def load(_opts), do: nil

    @impl true
    def save(_snapshot, _opts), do: :ok
  end

  test "monitor_store_child_specs omits callback-only stores" do
    assert UOF.SDK.monitor_store_child_specs({CallbackOnlyMonitorStore, []}) == []
  end

  test "monitor_store_child_specs includes stores with child specs" do
    assert UOF.SDK.monitor_store_child_specs({StartableMonitorStore, key: :a}) == [{StartableMonitorStore, key: :a}]
  end

  test "child_specs passes normalized producer specs to system and content pipelines" do
    conn = [host: "stgmq.betradar.com", username: "tok", password: "", ssl_options: []]
    config = Config.load(handler: LogHandler, transport: {:amqp, connection: conn})

    assert [{SystemPipeline, system_opts}, {ContentPipeline, content_opts}] = UOF.SDK.child_specs(config)
    assert system_opts[:name] == SystemPipeline
    assert {BroadwayRabbitMQ.Producer, system_producer_opts} = system_opts[:producer]
    assert system_producer_opts[:connection] == {:custom_pool, Connection, Connection}

    assert content_opts[:name] == ContentPipeline
    assert content_opts[:handler] == LogHandler
    assert {BroadwayRabbitMQ.Producer, content_producer_opts} = content_opts[:producer]
    assert content_producer_opts[:connection] == system_producer_opts[:connection]
  end

  test "child_specs passes node_id to both pipelines" do
    config = Config.load(handler: LogHandler, node_id: 7)

    assert [{SystemPipeline, system_opts}, {ContentPipeline, content_opts}] = UOF.SDK.child_specs(config)
    assert system_opts[:node_id] == 7
    assert content_opts[:node_id] == 7
  end

  test "child_specs passes concurrency through to the content pipeline" do
    assert [{SystemPipeline, _system}, {ContentPipeline, default}] =
             UOF.SDK.child_specs(Config.load(handler: LogHandler))

    assert default[:concurrency] == 10

    config = Config.load(handler: LogHandler, concurrency: 50)
    [{SystemPipeline, _system}, {ContentPipeline, opts}] = UOF.SDK.child_specs(config)
    assert opts[:concurrency] == 50
  end

  test "supervises a Pulsar client only for the Pulsar transport" do
    assert {:ok, {_flags, amqp_children}} = UOF.SDK.init(handler: LogHandler)
    refute Enum.any?(amqp_children, &(&1.id == :uof_sdk_pulsar))

    assert {:ok, {_flags, pulsar_children}} =
             UOF.SDK.init(
               handler: LogHandler,
               transport: {:pulsar, host: "pulsar://localhost:6650", topic: "uof-feed", subscription: "uof-sdk"}
             )

    assert Enum.any?(pulsar_children, fn child ->
             child.id == :uof_sdk_pulsar and
               child.start ==
                 {Pulsar.Client, :start_link, [[name: :uof_sdk_pulsar, host: "pulsar://localhost:6650"]]}
           end)
  end

  test "supervises the shared AMQP connection after the monitor and before both pipelines" do
    assert {:ok, {flags, children}} = UOF.SDK.init(handler: LogHandler)
    assert flags.strategy == :rest_for_one

    assert Enum.map(children, & &1.id) == [
             UOF.SDK.ProducerMonitor.Store.ETS,
             UOF.SDK.ProducerMonitor,
             Connection,
             SystemPipeline,
             ContentPipeline
           ]
  end
end

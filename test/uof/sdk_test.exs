defmodule UOF.SDKTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.Config
  alias UOF.SDK.ContentPipeline
  alias UOF.SDK.LogHandler
  alias UOF.SDK.ProducerMonitor
  alias UOF.SDK.ProducerMonitor.Store
  alias UOF.SDK.SystemPipeline

  @pulsar [host: "pulsar://localhost:6650", topic: "uof-feed", subscription: "uof-sdk"]

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

  defp load(overrides \\ []), do: Config.load(Keyword.merge([handler: LogHandler, pulsar: @pulsar], overrides))

  test "child_specs passes normalized producer specs to system and content pipelines" do
    config = load()

    assert [{SystemPipeline, system_opts}, {ContentPipeline, content_opts}] = UOF.SDK.child_specs(config)
    assert system_opts[:name] == SystemPipeline
    assert system_opts[:producer] == config.system_producer
    assert system_opts[:monitor] == ProducerMonitor

    assert content_opts[:name] == ContentPipeline
    assert content_opts[:handler] == LogHandler
    assert content_opts[:producer] == config.content_producer
    # Feed continuity is observed on the system subscription only.
    refute Keyword.has_key?(content_opts, :monitor)
  end

  test "child_specs passes node_id to both pipelines" do
    assert [{SystemPipeline, system_opts}, {ContentPipeline, content_opts}] = UOF.SDK.child_specs(load(node_id: 7))
    assert system_opts[:node_id] == 7
    assert content_opts[:node_id] == 7
  end

  test "child_specs passes concurrency through to the content pipeline" do
    assert [{SystemPipeline, _system}, {ContentPipeline, default}] = UOF.SDK.child_specs(load())
    assert default[:concurrency] == 10

    [{SystemPipeline, _system}, {ContentPipeline, opts}] = UOF.SDK.child_specs(load(concurrency: 50))
    assert opts[:concurrency] == 50
  end

  test "supervises the Pulsar client after the monitor and before both pipelines" do
    assert {:ok, {flags, children}} = UOF.SDK.init(handler: LogHandler, pulsar: @pulsar)
    assert flags.strategy == :rest_for_one

    assert Enum.map(children, & &1.id) == [
             UOF.SDK.ProducerMonitor.Store.ETS,
             ProducerMonitor,
             :uof_sdk_pulsar,
             SystemPipeline,
             ContentPipeline
           ]

    assert Enum.any?(children, fn child ->
             child.id == :uof_sdk_pulsar and
               child.start ==
                 {Pulsar.Client, :start_link, [[name: :uof_sdk_pulsar, host: "pulsar://localhost:6650"]]}
           end)
  end
end

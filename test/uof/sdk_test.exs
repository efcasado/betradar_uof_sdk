defmodule UOF.SDKTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.CheckpointStore
  alias UOF.SDK.Config
  alias UOF.SDK.ContentPipeline
  alias UOF.SDK.SystemPipeline

  defmodule CallbackOnlyCheckpointStore do
    @moduledoc false
    @behaviour CheckpointStore

    @impl true
    def get(_producer_id), do: :none

    @impl true
    def put(_producer_id, _timestamp), do: :ok

    @impl true
    def delete(_producer_id), do: :ok

    @impl true
    def get_state, do: %{}

    @impl true
    def put_state(_producer_id, _state), do: :ok

    @impl true
    def get_connection_tokens, do: %{}

    @impl true
    def put_connection_token(_namespace, _token), do: :ok
  end

  defmodule StartableCheckpointStore do
    @moduledoc false
    @behaviour CheckpointStore

    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl GenServer
    def init(opts), do: {:ok, opts}

    @impl true
    def get(_producer_id), do: :none

    @impl true
    def put(_producer_id, _timestamp), do: :ok

    @impl true
    def delete(_producer_id), do: :ok

    @impl true
    def get_state, do: %{}

    @impl true
    def put_state(_producer_id, _state), do: :ok

    @impl true
    def get_connection_tokens, do: %{}

    @impl true
    def put_connection_token(_namespace, _token), do: :ok
  end

  test "checkpoint_store_child_specs omits callback-only stores" do
    assert UOF.SDK.checkpoint_store_child_specs(CallbackOnlyCheckpointStore) == []
  end

  test "checkpoint_store_child_specs includes stores with child specs" do
    assert UOF.SDK.checkpoint_store_child_specs(StartableCheckpointStore) == [StartableCheckpointStore]
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

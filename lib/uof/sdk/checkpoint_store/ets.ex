defmodule UOF.SDK.CheckpointStore.ETS do
  @moduledoc """
  Default `UOF.SDK.CheckpointStore` — in-memory, lost on VM restart (falls
  back to full recovery on next start).

  The table outlives `ProducerMonitor`/pipeline crashes within the same VM
  (the store is supervised before them with `:rest_for_one`), so producer
  state and connection tokens still enable crash-restart resume without a
  recovery.
  """

  @behaviour UOF.SDK.CheckpointStore

  use GenServer

  alias UOF.SDK.CheckpointStore

  @table __MODULE__

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl GenServer
  def init(_opts) do
    :ets.new(@table, [
      :named_table,
      :public,
      :set,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok, %{}}
  end

  @impl CheckpointStore
  def get(producer_id) do
    case :ets.lookup(@table, {:checkpoint, producer_id}) do
      [{_key, timestamp}] -> {:ok, timestamp}
      [] -> :none
    end
  end

  @impl CheckpointStore
  def put(producer_id, timestamp) do
    true = :ets.insert(@table, {{:checkpoint, producer_id}, timestamp})
    :ok
  end

  @impl CheckpointStore
  def delete(producer_id) do
    true = :ets.delete(@table, {:checkpoint, producer_id})
    :ok
  end

  @impl CheckpointStore
  def get_state do
    @table
    |> :ets.match({{:state, :"$1"}, :"$2"})
    |> Map.new(fn [id, state] -> {id, state} end)
  end

  @impl CheckpointStore
  def put_state(producer_id, state) do
    true = :ets.insert(@table, {{:state, producer_id}, state})
    :ok
  end

  @impl CheckpointStore
  def get_connection_tokens do
    @table
    |> :ets.match({{:connection_token, :"$1"}, :"$2"})
    |> Map.new(fn [namespace, token] -> {namespace, token} end)
  end

  @impl CheckpointStore
  def put_connection_token(namespace, token) do
    true = :ets.insert(@table, {{:connection_token, namespace}, token})
    :ok
  end
end

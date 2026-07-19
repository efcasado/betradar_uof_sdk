defmodule UOF.SDK.ProducerMonitor.Store.ETS do
  @moduledoc """
  In-memory `UOF.SDK.ProducerMonitor.Store` implementation.

  Its state survives monitor and pipeline crashes while this store process
  remains alive, but is lost when the VM stops.
  """

  @behaviour UOF.SDK.ProducerMonitor.Store

  use GenServer

  alias UOF.SDK.ProducerMonitor.Store
  alias UOF.SDK.ProducerMonitor.Store.ConnectionState
  alias UOF.SDK.ProducerMonitor.Store.ProducerState

  @table __MODULE__
  @connection_key :connection

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl GenServer
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
    {:ok, %{}}
  end

  @impl Store
  def load_connection_state do
    case :ets.lookup(@table, @connection_key) do
      [{@connection_key, connection}] -> connection
      [] -> %ConnectionState{}
    end
  end

  @impl Store
  def load_producer_states do
    @table
    |> :ets.match_object({{:producer, :_}, :_})
    |> Map.new(fn {{:producer, id}, producer_state} -> {id, producer_state} end)
  end

  @impl Store
  def commit_connection_change(tokens) when is_map(tokens) do
    connection = load_connection_state()
    changed = %ConnectionState{tokens: tokens, generation: connection.generation + 1}
    true = :ets.insert(@table, {@connection_key, changed})
    changed
  end

  @impl Store
  def advance_checkpoint(id, timestamp) when is_integer(timestamp) do
    update_producer(id, fn
      %ProducerState{checkpoint: checkpoint} = state when is_integer(checkpoint) and checkpoint >= timestamp ->
        state

      state ->
        %{state | checkpoint: timestamp}
    end)
  end

  @impl Store
  def require_recovery(id) do
    update_producer(id, &%{&1 | synchronized_generation: nil})
  end

  @impl Store
  def mark_synchronized(id, generation) when is_integer(generation) and generation >= 0 do
    update_producer(id, &%{&1 | synchronized_generation: generation})
  end

  defp update_producer(id, update) do
    state =
      case :ets.lookup(@table, {:producer, id}) do
        [{{:producer, ^id}, state}] -> state
        [] -> %ProducerState{}
      end

    updated = update.(state)

    if updated != state do
      true = :ets.insert(@table, {{:producer, id}, updated})
    end

    updated
  end
end

defmodule UOF.SDK.ProducerMonitor.Store.ETS do
  @moduledoc """
  In-memory `UOF.SDK.ProducerMonitor.Store` implementation.

  Its state survives monitor and pipeline crashes while this store process
  remains alive, but is lost when the VM stops.
  """

  @behaviour UOF.SDK.ProducerMonitor.Store

  use GenServer

  alias UOF.SDK.ProducerMonitor.Store
  alias UOF.SDK.ProducerMonitor.Store.ProducerProgress
  alias UOF.SDK.ProducerMonitor.Store.Session

  @table __MODULE__
  @session_key :session

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl GenServer
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
    {:ok, %{}}
  end

  @impl Store
  def load_session do
    case :ets.lookup(@table, @session_key) do
      [{@session_key, session}] -> session
      [] -> %Session{}
    end
  end

  @impl Store
  def load_producer_progress do
    @table
    |> :ets.match_object({{:producer, :_}, :_})
    |> Map.new(fn {{:producer, id}, progress} -> {id, progress} end)
  end

  @impl Store
  def commit_session_change(tokens) when is_map(tokens) do
    session = load_session()
    changed = %Session{tokens: tokens, generation: session.generation + 1}
    true = :ets.insert(@table, {@session_key, changed})
    changed
  end

  @impl Store
  def advance_checkpoint(id, timestamp) when is_integer(timestamp) do
    update_producer(id, fn
      %ProducerProgress{checkpoint: checkpoint} = progress
      when is_integer(checkpoint) and checkpoint >= timestamp ->
        progress

      progress ->
        %{progress | checkpoint: timestamp}
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
    progress =
      case :ets.lookup(@table, {:producer, id}) do
        [{{:producer, ^id}, progress}] -> progress
        [] -> %ProducerProgress{}
      end

    updated = update.(progress)

    if updated != progress do
      true = :ets.insert(@table, {{:producer, id}, updated})
    end

    updated
  end
end

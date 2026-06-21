defmodule UOF.SDK.CheckpointStore.ETS do
  @moduledoc """
  Default `UOF.SDK.CheckpointStore` backed by a public ETS table.

  Zero dependencies and fast — `get`/`put`/`delete` are direct ETS operations,
  not `GenServer` calls, so per-message checkpointing isn't a bottleneck. The
  `GenServer` exists only to own the table for its lifetime. Checkpoints are
  lost on VM restart, which simply falls back to full recovery on next start.
  """

  @behaviour UOF.SDK.CheckpointStore

  use GenServer

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

  @impl UOF.SDK.CheckpointStore
  def get(producer_id) do
    case :ets.lookup(@table, producer_id) do
      [{^producer_id, timestamp}] -> {:ok, timestamp}
      [] -> :none
    end
  end

  @impl UOF.SDK.CheckpointStore
  def put(producer_id, timestamp) do
    :ets.insert(@table, {producer_id, timestamp})
    :ok
  end

  @impl UOF.SDK.CheckpointStore
  def delete(producer_id) do
    :ets.delete(@table, producer_id)
    :ok
  end
end

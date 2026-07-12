defmodule UOF.SDK.MonitorStore.ETS do
  @moduledoc """
  In-memory `UOF.SDK.MonitorStore` implementation.

  Its snapshot survives monitor and pipeline crashes while this store process
  remains alive, but is lost when the VM stops.
  """

  @behaviour UOF.SDK.MonitorStore

  use GenServer

  alias UOF.SDK.MonitorSnapshot
  alias UOF.SDK.MonitorStore

  @table __MODULE__
  @snapshot_key :snapshot

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl GenServer
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
    {:ok, %{}}
  end

  @impl MonitorStore
  def load do
    case :ets.lookup(@table, @snapshot_key) do
      [{@snapshot_key, snapshot}] -> snapshot
      [] -> %MonitorSnapshot{}
    end
  end

  @impl MonitorStore
  def save(%MonitorSnapshot{} = snapshot) do
    :ets.insert(@table, {@snapshot_key, snapshot})
    :ok
  end
end

defmodule UOF.SDK.ProducerMonitor.Store.ETS do
  @moduledoc """
  In-memory `UOF.SDK.ProducerMonitor.Store` implementation.

  Its snapshot survives monitor and pipeline crashes while this store process
  remains alive, but is lost when the VM stops.
  """

  @behaviour UOF.SDK.ProducerMonitor.Store

  use GenServer

  alias UOF.SDK.ProducerMonitor.Snapshot
  alias UOF.SDK.ProducerMonitor.Store

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

  @impl Store
  def load do
    case :ets.lookup(@table, @snapshot_key) do
      [{@snapshot_key, snapshot}] -> snapshot
      [] -> %Snapshot{}
    end
  end

  @impl Store
  def save(%Snapshot{} = snapshot) do
    :ets.insert(@table, {@snapshot_key, snapshot})
    :ok
  end
end

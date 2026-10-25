defmodule UOF.SDK.ProducerMonitor.Store.ETS do
  @moduledoc """
  In-memory `UOF.SDK.ProducerMonitor.Store` implementation.

  The snapshot survives monitor and pipeline crashes while this store process
  remains alive, but is lost when the VM stops. It is local to the VM, so
  instances on other VMs do not share it.
  """

  @behaviour UOF.SDK.ProducerMonitor.Store

  use GenServer

  alias UOF.SDK.ProducerMonitor.Store

  @table __MODULE__

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl GenServer
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
    {:ok, %{}}
  end

  @impl Store
  def load(_opts) do
    case :ets.lookup(@table, :snapshot) do
      [{_key, snapshot}] -> snapshot
      [] -> nil
    end
  end

  @impl Store
  def save(snapshot, _opts) when is_binary(snapshot) do
    true = :ets.insert(@table, {:snapshot, snapshot})
    :ok
  end
end

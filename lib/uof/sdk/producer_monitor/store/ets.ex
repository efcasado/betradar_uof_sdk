defmodule UOF.SDK.ProducerMonitor.Store.ETS do
  @moduledoc """
  In-memory `UOF.SDK.ProducerMonitor.Store` implementation.

  Snapshots survive monitor and pipeline crashes while this store process
  remains alive, but are lost when the VM stops. The `:key` option (default
  `:default`) selects the snapshot.
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
  def load(opts) do
    case :ets.lookup(@table, key(opts)) do
      [{_key, snapshot}] -> snapshot
      [] -> nil
    end
  end

  @impl Store
  def save(snapshot, opts) when is_binary(snapshot) do
    true = :ets.insert(@table, {key(opts), snapshot})
    :ok
  end

  defp key(opts), do: Keyword.get(opts, :key, :default)
end

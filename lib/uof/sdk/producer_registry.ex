defmodule UOF.SDK.ProducerRegistry do
  @moduledoc """
  ETS-backed store of per-producer `UOF.SDK.Producer` state.

  Reads (`get/1`, `all/0`) are direct ETS lookups — fast and concurrent, so
  health checks never queue behind the monitor. Writes (`register/1`,
  `update/2`) come only from `UOF.SDK.ProducerMonitor` (a single writer), so no
  serialization through this process is needed; the `GenServer` exists only to
  own the table for its lifetime.
  """

  use GenServer

  alias UOF.SDK.Producer

  @table __MODULE__

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl GenServer
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
    {:ok, %{}}
  end

  @doc "Insert or replace a producer's state."
  @spec register(Producer.t()) :: :ok
  def register(%Producer{} = producer) do
    :ets.insert(@table, {producer.id, producer})
    :ok
  end

  @doc "Fetch a producer's state by id."
  @spec get(integer()) :: {:ok, Producer.t()} | :error
  def get(id) do
    case :ets.lookup(@table, id) do
      [{^id, producer}] -> {:ok, producer}
      [] -> :error
    end
  end

  @doc "All producer states, ordered by id."
  @spec all() :: [Producer.t()]
  def all do
    @table
    |> :ets.tab2list()
    |> Enum.map(&elem(&1, 1))
    |> Enum.sort_by(& &1.id)
  end

  @doc """
  Read-modify-write a producer's state. Returns the updated producer, or
  `:error` if it is not registered. Single-writer (the monitor), so safe.
  """
  @spec update(integer(), (Producer.t() -> Producer.t())) :: {:ok, Producer.t()} | :error
  def update(id, fun) when is_function(fun, 1) do
    with {:ok, producer} <- get(id) do
      updated = fun.(producer)
      register(updated)
      {:ok, updated}
    end
  end
end

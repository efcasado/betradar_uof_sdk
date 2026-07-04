defmodule UOF.SDK.ProducerMonitor do
  @moduledoc """
  Tracks producer health on the two independent axes Betradar defines and drives
  producer up/down state, recovery requests and the `handle_producer_status/1`
  callback.

  ## The two down-axes

    * **Delivery / alive** — when `alive` heartbeats stop (older than
      `inactivity_ms`) or `subscribed=0` arrives, the producer is marked down
      (`:alive_interval_violation`) and a **recovery is requested**. It returns
      up via `:returned_from_inactivity` (or `:first_recovery_completed` the
      first time) when recovery completes.

    * **Processing lag** — when the messages being processed were generated more
      than `inactivity_ms` ago, the producer is marked down + `delayed?`
      (`:processing_queue_delay_violation`) but **no recovery is issued**; the
      remote producer is healthy. It returns up via
      `:processing_queue_delay_stabilized` once processing catches up.

  Recovery itself is performed by a separate component (Slice B/B4); this module
  calls the injected `:recover` function and is told of completion via
  `recovery_completed/3`. The monitor is the single writer of
  `UOF.SDK.ProducerRegistry`.

  The event functions (`alive/4`, `message/3`) are called by the Broadway
  pipeline; that wiring lands in B5.
  """

  use GenServer

  require Logger

  alias UOF.SDK.{Producer, ProducerRegistry}

  @default_inactivity_ms 20_000
  @default_tick_ms 1_000

  # Dedup table for connection-pid observations (atomic, set once per pid).
  @connections __MODULE__.Connections

  ## Client API --------------------------------------------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc "Record an `alive` heartbeat. `subscribed?` false means recovery is needed."
  def alive(server \\ __MODULE__, producer_id, gen_timestamp, subscribed?) do
    GenServer.cast(server, {:alive, producer_id, gen_timestamp, subscribed?})
  end

  @doc "Record a processed content message's generation timestamp."
  def message(server \\ __MODULE__, producer_id, gen_timestamp) do
    GenServer.cast(server, {:message, producer_id, gen_timestamp})
  end

  @doc "Recovery has completed for `producer_id` (correlated by `request_id`)."
  def recovery_completed(server \\ __MODULE__, producer_id, request_id) do
    GenServer.cast(server, {:recovery_completed, producer_id, request_id})
  end

  @doc """
  Observe the AMQP connection a message arrived on. The first time a given
  connection pid is seen, every producer is recovered — this fires on the
  initial connection and again on each reconnect (a reconnect always yields a
  new connection pid), closing the message-gap a reconnect leaves behind. The
  ETS dedup is atomic, so concurrent processors fire it exactly once.
  """
  def observe_connection(server \\ __MODULE__, conn_pid) do
    if :ets.insert_new(@connections, {conn_pid, true}) do
      GenServer.cast(server, {:connection_established, conn_pid})
    end

    :ok
  end

  ## Server ------------------------------------------------------------------

  @impl true
  def init(opts) do
    ensure_connections_table()
    registry = Keyword.get(opts, :registry, ProducerRegistry)
    for producer <- Keyword.get(opts, :producers, []), do: registry.register(producer)

    state = %{
      registry: registry,
      handler: Keyword.get(opts, :handler),
      recover: Keyword.get(opts, :recover, &log_recover/1),
      now_fun: Keyword.get(opts, :now_fun, &now_ms/0),
      inactivity_ms: Keyword.get(opts, :inactivity_ms, @default_inactivity_ms),
      tick_ms: Keyword.get(opts, :tick_ms, @default_tick_ms),
      first_recovery_done: MapSet.new()
    }

    schedule_tick(state)
    {:ok, state}
  end

  @impl true
  def handle_cast({:alive, id, gen_ts, subscribed?}, state) do
    with_producer(state, id, &on_alive(state, &1, gen_ts, subscribed?))
    {:noreply, state}
  end

  def handle_cast({:message, id, gen_ts}, state) do
    state.registry.update(id, fn p ->
      %{
        p
        | last_message_timestamp: max_ts(p.last_message_timestamp, gen_ts),
          last_processed_message_gen_timestamp:
            max_ts(p.last_processed_message_gen_timestamp, gen_ts)
      }
    end)

    {:noreply, state}
  end

  def handle_cast({:connection_established, _conn_pid}, state) do
    # A (re)connect leaves a gap; recover every producer not already recovering.
    for producer <- state.registry.all(), not producer.recovering? do
      trigger_recovery(state, producer, :connection_down)
    end

    {:noreply, state}
  end

  def handle_cast({:recovery_completed, id, _request_id}, state) do
    case state.registry.get(id) do
      {:ok, producer} ->
        {first?, state} = pop_first_recovery(state, id)
        reason = if first?, do: :first_recovery_completed, else: :returned_from_inactivity

        apply_status(state, producer, %{
          down?: false,
          delayed?: false,
          recovering?: false,
          recovery_id: nil,
          reason: reason
        })

        {:noreply, state}

      :error ->
        {:noreply, state}
    end
  end

  @impl true
  def handle_call(:sync, _from, state), do: {:reply, :ok, state}

  @impl true
  def handle_info(:tick, state) do
    now = state.now_fun.()
    for producer <- state.registry.all(), do: check(state, producer, now)
    schedule_tick(state)
    {:noreply, state}
  end

  ## transitions -------------------------------------------------------------

  defp on_alive(state, producer, gen_ts, subscribed?) do
    producer = %{
      producer
      | last_alive_at: state.now_fun.(),
        last_message_timestamp: max_ts(producer.last_message_timestamp, gen_ts)
    }

    cond do
      (not subscribed? or producer.down?) and not producer.recovering? ->
        trigger_recovery(state, producer, producer.reason)

      true ->
        state.registry.register(producer)
    end
  end

  defp check(state, producer, now) do
    cond do
      producer.recovering? ->
        :ok

      alive_violation?(producer, now, state.inactivity_ms) ->
        trigger_recovery(state, producer, :alive_interval_violation)

      processing_violation?(producer, now, state.inactivity_ms) and not producer.delayed? ->
        apply_status(state, producer, %{
          down?: true,
          delayed?: true,
          reason: :processing_queue_delay_violation,
          processing_queue_delay: now - producer.last_processed_message_gen_timestamp
        })

      producer.delayed? and not processing_violation?(producer, now, state.inactivity_ms) ->
        apply_status(state, producer, %{
          down?: false,
          delayed?: false,
          reason: :processing_queue_delay_stabilized,
          processing_queue_delay: nil
        })

      true ->
        :ok
    end
  end

  # Mark down + recovering and ask the recovery component to recover.
  defp trigger_recovery(state, before, reason) do
    after_ = %{before | down?: true, recovering?: true, reason: reason}
    state.registry.register(after_)
    maybe_notify(state, before, after_)
    state.recover.(after_)
    :ok
  end

  defp apply_status(state, before, attrs) do
    after_ = struct(before, attrs)
    state.registry.register(after_)
    maybe_notify(state, before, after_)
    :ok
  end

  defp maybe_notify(state, before, after_) do
    if status_changed?(before, after_), do: notify(state, after_)
  end

  defp status_changed?(a, b),
    do: {a.down?, a.delayed?, a.reason} != {b.down?, b.delayed?, b.reason}

  defp notify(%{handler: nil}, _producer), do: :ok
  defp notify(%{handler: handler}, producer), do: handler.handle_producer_status(producer)

  ## predicates / helpers ----------------------------------------------------

  defp alive_violation?(%Producer{last_alive_at: nil}, _now, _ms), do: false
  defp alive_violation?(%Producer{last_alive_at: t}, now, ms), do: now - t > ms

  defp processing_violation?(%Producer{last_processed_message_gen_timestamp: nil}, _now, _ms),
    do: false

  defp processing_violation?(%Producer{last_processed_message_gen_timestamp: t}, now, ms),
    do: now - t > ms

  defp with_producer(state, id, fun) do
    case state.registry.get(id) do
      {:ok, producer} -> fun.(producer)
      :error -> :ok
    end
  end

  defp pop_first_recovery(state, id) do
    if MapSet.member?(state.first_recovery_done, id) do
      {false, state}
    else
      {true, %{state | first_recovery_done: MapSet.put(state.first_recovery_done, id)}}
    end
  end

  defp max_ts(nil, b), do: b
  defp max_ts(a, nil), do: a
  defp max_ts(a, b), do: max(a, b)

  defp ensure_connections_table do
    case :ets.whereis(@connections) do
      :undefined -> :ets.new(@connections, [:named_table, :public, :set])
      _ref -> @connections
    end
  end

  defp schedule_tick(state), do: Process.send_after(self(), :tick, state.tick_ms)

  defp now_ms, do: System.system_time(:millisecond)

  defp log_recover(producer) do
    Logger.info("UOF.SDK.ProducerMonitor: recovery requested for producer #{producer.id}")
    :ok
  end
end

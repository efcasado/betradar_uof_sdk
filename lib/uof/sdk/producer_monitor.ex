defmodule UOF.SDK.ProducerMonitor do
  @moduledoc """
  Tracks producer health and orchestrates recovery — two concerns that are
  tightly coupled in the UOF protocol.

  ## Health monitoring

  Two independent "down" axes are tracked per producer:

    * **Delivery / alive** — when `alive` heartbeats stop (older than
      `inactivity_ms`) or `subscribed=0` arrives, the producer is marked down
      and recovery is initiated. It returns up via `:returned_from_inactivity`
      (or `:first_recovery_completed` the first time) when recovery completes.

    * **Processing lag** — when the newest content-queue timestamp processed by
      the content pipeline was generated more than `max_processing_delay_ms`
      ago, the producer is marked down + `delayed?` but **no recovery is
      issued** — the remote producer is healthy. Content-session `alive`
      messages refresh this timestamp for quiet producers because they queue
      behind event content. It returns up via
      `:processing_queue_delay_stabilized` once processing catches up. This
      threshold is independent of
      `inactivity_ms` so consumer-lag tolerance can be tuned separately from the
      alive-gap/recovery trigger.

  ## Recovery orchestration

  Recovery checkpoints are owned here, not by the Broadway pipeline. Subscribed
  `alive` heartbeats advance the checkpoint only after the producer is already
  up. Incremental recovery subtracts `recovery_overlap_ms` from the stored
  checkpoint, intentionally replaying a bounded window to cover concurrent
  processing and distributed-consumer skew.

  When a delivery gap is detected, this module:

    * computes the `after:` timestamp from `UOF.SDK.CheckpointStore` (clamped
      to the producer's `recovery_window_minutes`; a full recovery when there
      is no checkpoint),
    * issues `UOF.API.Recovery.recover/2` with a fresh `request_id`, emitting
      a `[:uof_sdk, :recovery, :initiated]` telemetry event,
    * keeps at most one in-flight recovery per producer,
    * **reissues** with the *original* timestamp if no `snapshot_complete`
      arrives within `max_recovery_ms` (the stall guard),
    * **retries** after `min_interval_ms` if the API call fails or the request
      is rejected (a non-accepted `<response>` envelope), and
    * marks the producer up on the matching `snapshot_complete`.

  The defaults for `min_interval_ms` / `max_recovery_ms` mirror the official
  SDK and are throttling-safe; change with care.
  """

  use GenServer

  alias UOF.Schemas.Common.Response
  alias UOF.SDK.Producer

  require Logger

  @default_inactivity_ms 20_000
  @default_max_processing_delay_ms 20_000
  @default_tick_ms 1_000
  @default_min_interval_ms 30_000
  @default_max_recovery_ms 5 * 60_000
  @default_recovery_overlap_ms 5 * 60_000
  @startup_connection_count 2

  ## Client API ---------------------------------------------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc "All known producers, ordered by id."
  def producers(server \\ __MODULE__) do
    GenServer.call(server, :all_producers)
  end

  @doc "Get a single producer by id."
  def producer(server \\ __MODULE__, id) do
    GenServer.call(server, {:get_producer, id})
  end

  @doc "Record an `alive` heartbeat. `subscribed?` false means recovery is needed."
  def alive(server \\ __MODULE__, producer_id, gen_timestamp, subscribed?) do
    GenServer.cast(server, {:alive, producer_id, gen_timestamp, subscribed?})
  end

  @doc "Record a generation timestamp processed by the content pipeline."
  def message(server \\ __MODULE__, producer_id, gen_timestamp) do
    GenServer.cast(server, {:message, producer_id, gen_timestamp})
  end

  @doc "Feed a `snapshot_complete` for correlation against the in-flight recovery."
  def snapshot_complete(server \\ __MODULE__, producer_id, request_id) do
    GenServer.cast(server, {:snapshot_complete, producer_id, request_id})
  end

  @doc """
  Manually trigger a recovery for `producer_id` (e.g. from an admin UI), going
  through the normal recovery lifecycle: the producer is marked down and
  recovering, the completion is correlated via `snapshot_complete`, and the
  stall guard reissues if it goes missing. Pass `full: true` to ignore the
  checkpoint and request a full snapshot. Refused when a recovery is already
  in flight.
  """
  @spec recover(GenServer.server(), integer(), keyword()) ::
          :ok | {:error, :already_recovering | :unknown_producer}
  def recover(server \\ __MODULE__, producer_id, opts) do
    GenServer.call(server, {:recover, producer_id, opts})
  end

  @doc """
  Observe the AMQP connection a message arrived on.

  Startup recovery waits until both the system and content connection namespaces
  have been observed, so replay starts only after both consumers are ready. After
  startup, a token change in any namespace recovers every producer to close the
  message-gap a reconnect leaves behind.
  """
  def observe_connection(server \\ __MODULE__, conn_pid) do
    GenServer.cast(server, {:observe_connection, conn_pid})
  end

  ## Server ------------------------------------------------------------------

  @impl true
  def init(opts) do
    producers =
      opts
      |> Keyword.get(:producers, [])
      |> Map.new(fn p -> {p.id, p} end)

    state = %{
      producers: producers,
      handler: Keyword.get(opts, :handler),
      now_fun: Keyword.get(opts, :now_fun, &now_ms/0),
      inactivity_ms: Keyword.get(opts, :inactivity_ms, @default_inactivity_ms),
      max_processing_delay_ms: Keyword.get(opts, :max_processing_delay_ms, @default_max_processing_delay_ms),
      tick_ms: Keyword.get(opts, :tick_ms, @default_tick_ms),
      first_recovery_done: MapSet.new(),
      connection_tokens: %{},
      recover_fun: Keyword.get(opts, :recover_fun, &UOF.API.Recovery.recover/2),
      checkpoint_store: Keyword.get(opts, :checkpoint_store, UOF.SDK.CheckpointStore.ETS),
      node_id: Keyword.get(opts, :node_id),
      min_interval_ms: Keyword.get(opts, :min_interval_ms, @default_min_interval_ms),
      max_recovery_ms: Keyword.get(opts, :max_recovery_ms, @default_max_recovery_ms),
      recovery_overlap_ms: Keyword.get(opts, :recovery_overlap_ms, @default_recovery_overlap_ms),
      gen_request_id:
        Keyword.get(opts, :gen_request_id, fn ->
          System.unique_integer([:positive, :monotonic])
        end),
      recoveries: %{}
    }

    schedule_tick(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:all_producers, _from, state) do
    result = state.producers |> Map.values() |> Enum.sort_by(& &1.id)
    {:reply, result, state}
  end

  def handle_call({:get_producer, id}, _from, state) do
    {:reply, Map.fetch(state.producers, id), state}
  end

  def handle_call({:recover, id, opts}, _from, state) do
    case Map.fetch(state.producers, id) do
      {:ok, %Producer{recovering?: true}} ->
        {:reply, {:error, :already_recovering}, state}

      {:ok, producer} ->
        after_ts = if !opts[:full], do: after_from_checkpoint(state, producer)
        {:reply, :ok, trigger_recovery(state, producer, :other, after_ts)}

      :error ->
        {:reply, {:error, :unknown_producer}, state}
    end
  end

  def handle_call(:sync, _from, state), do: {:reply, :ok, state}

  @impl true
  def handle_cast({:alive, id, gen_ts, subscribed?}, state) do
    state = with_producer(state, id, fn p -> on_alive(state, p, gen_ts, subscribed?) end)
    {:noreply, state}
  end

  def handle_cast({:message, id, gen_ts}, state) do
    state =
      with_producer(state, id, fn p ->
        updated = %{p | last_message_timestamp: max_ts(p.last_message_timestamp, gen_ts)}
        put_producer(state, updated)
      end)

    {:noreply, state}
  end

  def handle_cast({:observe_connection, {namespace, token}}, state) do
    {action, state} = track_connection(state, namespace, token)

    state =
      if action == :recover do
        recover_non_recovering_producers(state, :connection_down)
      else
        state
      end

    {:noreply, state}
  end

  def handle_cast({:observe_connection, token}, state) do
    {action, state} = track_connection(state, :default, token)

    state =
      if action == :recover do
        recover_non_recovering_producers(state, :connection_down)
      else
        state
      end

    {:noreply, state}
  end

  def handle_cast({:snapshot_complete, id, request_id}, state) do
    case Map.get(state.recoveries, id) do
      %{request_id: ^request_id} ->
        Logger.info("UOF.SDK.ProducerMonitor: producer #{id} recovery #{request_id} complete")
        state = %{state | recoveries: Map.delete(state.recoveries, id)}

        state =
          with_producer(state, id, fn producer ->
            {first?, state} = pop_first_recovery(state, id)
            reason = if first?, do: :first_recovery_completed, else: :returned_from_inactivity

            apply_status(state, producer, %{
              down?: false,
              delayed?: false,
              recovering?: false,
              recovery_id: nil,
              reason: reason
            })
          end)

        {:noreply, state}

      _other ->
        # stale request_id, unknown producer, or another node's completion
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:tick, state) do
    now = state.now_fun.()
    state = Enum.reduce(Map.values(state.producers), state, &check(&2, &1, now))
    schedule_tick(state)
    {:noreply, state}
  end

  def handle_info({:stall, id, request_id}, state) do
    case Map.get(state.recoveries, id) do
      %{request_id: ^request_id, after_ts: after_ts, producer: producer} ->
        Logger.warning("UOF.SDK.ProducerMonitor: producer #{id} recovery #{request_id} stalled; reissuing")

        state = %{state | recoveries: Map.delete(state.recoveries, id)}
        {:noreply, initiate(state, producer, after_ts)}

      _other ->
        {:noreply, state}
    end
  end

  def handle_info({:retry, producer, after_ts}, state) do
    if Map.has_key?(state.recoveries, producer.id) do
      {:noreply, state}
    else
      {:noreply, initiate(state, producer, after_ts)}
    end
  end

  ## Health transitions -------------------------------------------------------

  defp on_alive(state, producer, gen_ts, subscribed?) do
    producer = %{producer | last_alive_at: state.now_fun.()}

    if recovery_needed?(producer, subscribed?) do
      if startup_connection_recovery_pending?(state) do
        put_producer(state, producer)
      else
        trigger_recovery(state, producer, producer.reason)
      end
    else
      maybe_checkpoint_alive(state, producer, gen_ts, subscribed?)
      put_producer(state, producer)
    end
  end

  # A live producer needs recovery only on a genuine desync: the feed reports us
  # unsubscribed, or it's down for a delivery reason. A producer that's down
  # purely because *local* processing lags (`delayed?`) must not recover — the
  # remote feed is healthy and recovers when our consumer catches up. Without
  # this exclusion every alive re-triggers recovery for a slow consumer.
  defp recovery_needed?(%Producer{recovering?: true}, _subscribed?), do: false

  defp recovery_needed?(%Producer{down?: down?, delayed?: delayed?}, subscribed?) do
    not subscribed? or (down? and not delayed?)
  end

  defp check(state, producer, now) do
    cond do
      producer.recovering? ->
        state

      alive_violation?(producer, now, state.inactivity_ms) ->
        trigger_recovery(state, producer, :alive_interval_violation)

      processing_violation?(producer, now, state.max_processing_delay_ms) and not producer.delayed? ->
        apply_status(state, producer, %{
          down?: true,
          delayed?: true,
          reason: :processing_queue_delay_violation,
          processing_queue_delay: now - producer.last_message_timestamp
        })

      producer.delayed? and not processing_violation?(producer, now, state.max_processing_delay_ms) ->
        apply_status(state, producer, %{
          down?: false,
          delayed?: false,
          reason: :processing_queue_delay_stabilized,
          processing_queue_delay: nil
        })

      true ->
        state
    end
  end

  defp trigger_recovery(state, before, reason) do
    trigger_recovery(state, before, reason, after_from_checkpoint(state, before))
  end

  defp trigger_recovery(state, before, reason, after_ts) do
    after_ = %{before | down?: true, recovering?: true, reason: reason}
    state = put_producer(state, after_)
    maybe_notify(state, before, after_)
    initiate(state, after_, after_ts)
  end

  defp apply_status(state, before, attrs) do
    after_ = struct(before, attrs)
    state = put_producer(state, after_)
    maybe_notify(state, before, after_)
    state
  end

  defp maybe_notify(state, before, after_) do
    if status_changed?(before, after_), do: notify(state, after_)
  end

  defp status_changed?(a, b), do: {a.down?, a.delayed?, a.reason} != {b.down?, b.delayed?, b.reason}

  defp notify(%{handler: nil}, _producer), do: :ok
  defp notify(%{handler: handler}, producer), do: handler.handle_producer_status(producer)

  defp track_connection(state, namespace, token) do
    changed? = Map.get(state.connection_tokens, namespace) != token
    state = put_connection_token(state, namespace, token)

    if changed? and map_size(state.connection_tokens) == @startup_connection_count do
      {:recover, state}
    else
      {:ignore, state}
    end
  end

  defp startup_connection_recovery_pending?(state) do
    map_size(state.connection_tokens) in 1..(@startup_connection_count - 1)
  end

  defp put_connection_token(state, namespace, token) do
    %{state | connection_tokens: Map.put(state.connection_tokens, namespace, token)}
  end

  defp recover_non_recovering_producers(state, reason) do
    state.producers
    |> Map.values()
    |> Enum.reject(& &1.recovering?)
    |> Enum.reduce(state, fn p, acc -> trigger_recovery(acc, p, reason) end)
  end

  ## Recovery -----------------------------------------------------------------

  defp initiate(state, producer, after_ts) do
    request_id = state.gen_request_id.()
    opts = build_opts(after_ts, request_id, state.node_id)

    case safe_recover(state, producer.product, opts) do
      :ok ->
        emit_initiated(producer, request_id, after_ts)
        Process.send_after(self(), {:stall, producer.id, request_id}, state.max_recovery_ms)

        %{
          state
          | recoveries:
              Map.put(state.recoveries, producer.id, %{
                request_id: request_id,
                after_ts: after_ts,
                producer: producer
              })
        }

      :error ->
        Logger.warning(
          "UOF.SDK.ProducerMonitor: producer #{producer.id} recovery request failed; " <>
            "retrying in #{state.min_interval_ms}ms"
        )

        Process.send_after(self(), {:retry, producer, after_ts}, state.min_interval_ms)
        state
    end
  end

  # The HTTP layer decodes any parseable body — including rejection envelopes
  # (throttling, 403) — into `{:ok, %Response{}}` without surfacing the status
  # code, so acceptance must be checked here: mistaking a rejection for an
  # in-flight recovery leaves the producer waiting on a `snapshot_complete`
  # that was never scheduled.
  defp safe_recover(state, product, opts) do
    case state.recover_fun.(product, opts) do
      {:ok, %Response{response_code: code} = response} when code != "ACCEPTED" ->
        log_failure("#{code}: #{response.message || response.errors || "(no message)"}")

      {:ok, _response} ->
        :ok

      {:error, reason} ->
        log_failure(inspect(reason))
    end
  rescue
    exception -> log_failure(Exception.message(exception))
  end

  defp log_failure(detail) do
    Logger.warning("UOF.SDK.ProducerMonitor: recover request failed: #{detail}")
    :error
  end

  defp after_from_checkpoint(state, producer) do
    case state.checkpoint_store.get(producer.id) do
      {:ok, timestamp} ->
        timestamp
        |> checkpoint_after_overlap(state.recovery_overlap_ms)
        |> clamp_to_window(producer, state.now_fun.())

      :none ->
        nil
    end
  end

  defp put_producer(state, %Producer{} = producer) do
    %{state | producers: Map.put(state.producers, producer.id, producer)}
  end

  defp checkpoint_after_overlap(timestamp, overlap_ms), do: max(timestamp - overlap_ms, 0)

  defp put_checkpoint(state, id, timestamp) do
    case state.checkpoint_store.get(id) do
      {:ok, existing} when existing >= timestamp -> :ok
      _other -> state.checkpoint_store.put(id, timestamp)
    end
  end

  defp maybe_checkpoint_alive(_state, _producer, _timestamp, false), do: :ok
  defp maybe_checkpoint_alive(_state, %Producer{down?: true}, _timestamp, true), do: :ok
  defp maybe_checkpoint_alive(_state, %Producer{recovering?: true}, _timestamp, true), do: :ok
  defp maybe_checkpoint_alive(state, producer, timestamp, true), do: put_checkpoint(state, producer.id, timestamp)

  defp clamp_to_window(timestamp, %Producer{recovery_window_minutes: nil}, _now), do: timestamp

  defp clamp_to_window(timestamp, %Producer{recovery_window_minutes: minutes}, now) do
    earliest = now - minutes * 60_000
    max(timestamp, earliest)
  end

  defp build_opts(after_ts, request_id, node_id) do
    [request_id: request_id]
    |> maybe_put(:node_id, node_id)
    |> maybe_put(:after, after_ts)
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp emit_initiated(producer, request_id, after_ts) do
    :telemetry.execute(
      [:uof_sdk, :recovery, :initiated],
      %{system_time: System.system_time()},
      %{
        producer_id: producer.id,
        product: producer.product,
        request_id: request_id,
        recovery_from: after_ts
      }
    )

    Logger.info(
      "UOF.SDK.ProducerMonitor: producer #{producer.id} (#{producer.product}) recovery initiated " <>
        "request_id=#{request_id} after=#{inspect(after_ts)}"
    )
  end

  ## Predicates / helpers -----------------------------------------------------

  defp alive_violation?(%Producer{last_alive_at: nil}, _now, _ms), do: false
  defp alive_violation?(%Producer{last_alive_at: t}, now, ms), do: now - t > ms

  # "Behind" is measured against the newest timestamp processed by the content
  # pipeline: event content, plus content-session alives that queue behind it.
  # System alives are intentionally excluded because they arrive on a separate
  # queue and no longer prove content processing is caught up.
  defp processing_violation?(%Producer{last_message_timestamp: nil}, _now, _ms), do: false

  defp processing_violation?(%Producer{last_message_timestamp: t}, now, ms), do: now - t > ms

  defp with_producer(state, id, fun) do
    case Map.fetch(state.producers, id) do
      {:ok, producer} -> fun.(producer)
      :error -> state
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

  defp schedule_tick(state), do: Process.send_after(self(), :tick, state.tick_ms)

  defp now_ms, do: System.system_time(:millisecond)
end

defmodule UOF.SDK.ProducerMonitor do
  @moduledoc """
  Tracks producer health and orchestrates recovery — two concerns that are
  tightly coupled in the UOF protocol.

  ## Health monitoring

  Each producer has one lifecycle status:

    * `:down` and `:recovering` represent a delivery gap or initial
      synchronization. When `alive` heartbeats stop or `subscribed=0` arrives,
      recovery is initiated. Completion moves the producer to `:up`.

    * `:delayed` represents local processing lag. When the newest content-queue
      timestamp processed by the content pipeline was generated more than
      `max_processing_delay_ms` ago, no recovery is issued because the remote
      producer is healthy. Content-session `alive` messages refresh this
      timestamp for quiet producers because they queue behind event content.
      Processing catch-up returns it to `:up`.

    * `:resuming` represents a restart that can drain retained backlog without
      recovery. It becomes `:up` after processing catches up and current-session
      feed and connection activity confirm continuity.

  The processing threshold is independent of `inactivity_ms` so consumer-lag
  tolerance can be tuned separately from the alive-gap/recovery trigger.

  ## Recovery orchestration

  Recovery checkpoints are owned here, not by the Broadway pipeline. Subscribed
  `alive` heartbeats advance the checkpoint only after the producer is already
  up. Incremental recovery subtracts `recovery_overlap_ms` from the stored
  checkpoint, intentionally replaying a bounded window to cover concurrent
  processing and distributed-consumer skew.

  When a delivery gap is detected, this module:

    * computes the `after:` timestamp from `UOF.SDK.ProducerMonitor.Store` (clamped
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

  ## Restart resume

  The state store holds one atomic snapshot: producer checkpoints, the IDs that
  may resume without recovery, and committed connection tokens. At startup that
  snapshot is restored:

    * A resumable producer with a checkpoint starts as `:resuming`. It becomes
      `:up` after processing catches up, an `alive` is heard, and both pipeline
      connections are observed in the current process.
    * Persisted connection tokens are comparison baselines, not evidence that
      the current pipelines are ready. Once both pipelines are observed, a
      changed token removes every producer from the resumable set and starts
      recovery.
    * `subscribed=0` also starts recovery. Producers absent from the resumable
      set start `:down` and recover on their first subscribed `alive`.
  """

  use GenServer

  alias UOF.Schemas.Common.Response
  alias UOF.SDK.ProducerMonitor.Connections
  alias UOF.SDK.ProducerMonitor.Producer
  alias UOF.SDK.ProducerMonitor.Snapshot

  require Logger

  @default_inactivity_ms 20_000
  @default_max_processing_delay_ms 20_000
  @default_tick_ms 1_000
  @default_min_interval_ms 30_000
  @default_max_recovery_ms 5 * 60_000
  @default_recovery_overlap_ms 5 * 60_000

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

  @doc """
  Correlate `snapshot_complete` with an in-flight recovery.

  A matching completion moves the producer to `:up` and establishes the alive
  timeout anchor because receiving it proves current system-message delivery.
  """
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
    monitor_store = Keyword.get(opts, :monitor_store, UOF.SDK.ProducerMonitor.Store.ETS)
    snapshot = monitor_store.load()

    producers =
      opts
      |> Keyword.get(:producers, [])
      |> Map.new(fn p ->
        {p.id, restore_producer(p, snapshot)}
      end)

    state = %{
      producers: producers,
      handler: Keyword.get(opts, :handler),
      now_fun: Keyword.get(opts, :now_fun, &now_ms/0),
      inactivity_ms: Keyword.get(opts, :inactivity_ms, @default_inactivity_ms),
      max_processing_delay_ms: Keyword.get(opts, :max_processing_delay_ms, @default_max_processing_delay_ms),
      tick_ms: Keyword.get(opts, :tick_ms, @default_tick_ms),
      snapshot: snapshot,
      connections: Connections.new(snapshot.connection_tokens),
      recover_fun: Keyword.get(opts, :recover_fun, &UOF.API.Recovery.recover/2),
      monitor_store: monitor_store,
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
      {:ok, %Producer{status: :recovering}} ->
        {:reply, {:error, :already_recovering}, state}

      {:ok, producer} ->
        after_ts = if !opts[:full], do: after_from_checkpoint(state, producer)
        {:reply, :ok, trigger_recovery(state, producer, after_ts)}

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
        put_producer(state, Producer.observe_message(p, gen_ts))
      end)

    {:noreply, state}
  end

  def handle_cast({:observe_connection, {namespace, token}}, state) do
    connections = Connections.observe(state.connections, namespace, token)

    state =
      if Connections.ready?(connections) and Connections.changed?(connections) do
        connections = Connections.commit(connections)

        snapshot =
          state.snapshot
          |> Snapshot.require_recovery(Map.keys(state.producers))
          |> Map.put(:connection_tokens, connections.persisted)

        state = persist_snapshot(%{state | connections: connections, snapshot: snapshot})
        recover_non_recovering_producers(state)
      else
        %{state | connections: connections}
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
            commit_producer_transition(state, producer, Producer.complete_recovery(producer, state.now_fun.()))
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
    producer = Producer.observe_alive(producer, state.now_fun.())

    if producer.status != :recovering and (not subscribed? or producer.status == :down) do
      if startup_connection_recovery_pending?(state) do
        put_producer(state, producer)
      else
        trigger_recovery(state, producer)
      end
    else
      state = maybe_checkpoint_alive(state, producer, gen_ts, subscribed?)
      put_producer(state, producer)
    end
  end

  defp check(state, producer, now) do
    cond do
      producer.status == :recovering ->
        state

      alive_violation?(producer, now, state.inactivity_ms) and not startup_connection_recovery_pending?(state) ->
        trigger_recovery(state, producer)

      producer.status == :up and processing_violation?(producer, now, state.max_processing_delay_ms) ->
        commit_producer_transition(
          state,
          producer,
          Producer.mark_delayed(producer, now - producer.last_message_timestamp)
        )

      producer.status == :delayed and
          not processing_violation?(producer, now, state.max_processing_delay_ms) ->
        commit_producer_transition(state, producer, Producer.mark_up(producer))

      producer.status == :resuming and producer.last_alive_at != nil and
        Connections.ready?(state.connections) and
          not processing_violation?(producer, now, state.max_processing_delay_ms) ->
        commit_producer_transition(state, producer, Producer.mark_up(producer))

      true ->
        state
    end
  end

  defp trigger_recovery(state, before) do
    trigger_recovery(state, before, after_from_checkpoint(state, before))
  end

  defp trigger_recovery(state, before, after_ts) do
    after_ = Producer.start_recovery(before)
    state = commit_producer_transition(state, before, after_)
    initiate(state, after_, after_ts)
  end

  defp commit_producer_transition(state, before, after_) do
    state = put_producer(state, after_)
    persist_and_notify(state, before, after_)
  end

  # Persist restart safety and report the lifecycle transition.
  defp persist_and_notify(state, _before, after_) do
    state = persist_producer_state(state, after_)
    notify(state, after_)
    state
  end

  # A producer whose persisted state shows the remote feed was healthy at
  # shutdown (up, or down only from local processing lag) resumes as
  # `:resuming` — skipping the startup recovery — provided a checkpoint exists
  # to seed the processing-lag anchor (without one the up-transition check
  # would flip it up before anything drained). It holds there until a live
  # alive is heard and both pipeline connections are observed, since the
  # remote feed's health is assumed from stale state, not confirmed this
  # session. `subscribed=0` or a connection-token change observed while
  # draining still forces recovery; everything else starts `:down`.
  defp restore_producer(producer, snapshot) do
    if Snapshot.resumable?(snapshot, producer.id) do
      case Snapshot.checkpoint(snapshot, producer.id) do
        checkpoint when is_integer(checkpoint) ->
          %{producer | status: :resuming, last_message_timestamp: checkpoint}

        nil ->
          producer
      end
    else
      producer
    end
  end

  defp notify(%{handler: nil}, _producer), do: :ok
  defp notify(%{handler: handler}, producer), do: handler.handle_producer_status(producer)

  defp startup_connection_recovery_pending?(state) do
    Connections.active?(state.connections) and not Connections.ready?(state.connections)
  end

  defp recover_non_recovering_producers(state) do
    state.producers
    |> Map.values()
    |> Enum.reject(&(&1.status == :recovering))
    |> Enum.reduce(state, fn p, acc -> trigger_recovery(acc, p) end)
  end

  # Runtime status is not stored. `:resuming` is reconstructed at startup from
  # the resumable-producer set and checkpoint.
  defp persist_producer_state(state, after_) do
    resumable? = after_.status in [:up, :delayed]

    if Snapshot.resumable?(state.snapshot, after_.id) == resumable? do
      state
    else
      snapshot =
        if resumable? do
          Snapshot.mark_resumable(state.snapshot, after_.id)
        else
          Snapshot.require_recovery(state.snapshot, after_.id)
        end

      persist_snapshot(%{state | snapshot: snapshot})
    end
  end

  defp persist_snapshot(state) do
    :ok = state.monitor_store.save(state.snapshot)
    state
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
    case Snapshot.checkpoint(state.snapshot, producer.id) do
      timestamp when is_integer(timestamp) ->
        timestamp
        |> checkpoint_after_overlap(state.recovery_overlap_ms)
        |> clamp_to_window(producer, state.now_fun.())

      nil ->
        nil
    end
  end

  defp put_producer(state, %Producer{} = producer) do
    %{state | producers: Map.put(state.producers, producer.id, producer)}
  end

  defp checkpoint_after_overlap(timestamp, overlap_ms), do: max(timestamp - overlap_ms, 0)

  defp put_checkpoint(state, id, timestamp) do
    case Snapshot.checkpoint(state.snapshot, id) do
      existing when is_integer(existing) and existing >= timestamp ->
        state

      _other ->
        snapshot = Snapshot.put_checkpoint(state.snapshot, id, timestamp)
        persist_snapshot(%{state | snapshot: snapshot})
    end
  end

  defp maybe_checkpoint_alive(state, _producer, _timestamp, false), do: state

  defp maybe_checkpoint_alive(state, %Producer{status: :up} = producer, timestamp, true) do
    put_checkpoint(state, producer.id, timestamp)
  end

  defp maybe_checkpoint_alive(state, _producer, _timestamp, true), do: state

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

  defp schedule_tick(state), do: Process.send_after(self(), :tick, state.tick_ms)

  defp now_ms, do: System.system_time(:millisecond)
end

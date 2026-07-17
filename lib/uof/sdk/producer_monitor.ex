defmodule UOF.SDK.ProducerMonitor do
  @moduledoc """
  Tracks producer health and orchestrates recovery — two concerns that are
  tightly coupled in the UOF protocol.

  ## Health monitoring

  `UOF.SDK.ProducerMonitor.Producer` defines the lifecycle statuses. Delivery
  gaps move a producer from `:down` or `:up` to `:recovering`, and a matching
  recovery completion moves it to `:up`. Local processing lag moves `:up` to
  `:delayed` without recovery. A restart that can safely drain retained backlog
  starts at `:resuming` and becomes `:up` after current-session continuity is
  confirmed and processing catches up.

  The processing threshold is independent of `inactivity_ms` so consumer-lag
  tolerance can be tuned separately from the alive-gap/recovery trigger.

  ## Recovery orchestration

  Recovery checkpoints are owned here, not by the Broadway pipeline. Subscribed
  `alive` heartbeats advance the checkpoint only after the producer is already
  up. Incremental recovery subtracts `recovery_overlap_ms` from the stored
  checkpoint, intentionally replaying a bounded window to cover concurrent
  processing and distributed-consumer skew.

  A delivery gap marks the producer `:recovering`. The request is issued after
  the per-producer cooldown with a fresh `request_id` and a checkpoint-derived
  `after:` timestamp, clamped to the producer's recovery window. Issuing emits
  `[:uof_sdk, :recovery, :initiated]` telemetry and starts the stall deadline.
  Failed requests are retried; stalled requests are reissued from the original
  timestamp. A matching `snapshot_complete` moves the producer to `:up`.

  The recovery defaults follow the official SDK's recovery guidance and should
  be changed with care.

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

  ## Control-plane ownership (multi-instance Pulsar)

  With the Pulsar transport, the system subscription is Failover: the broker
  delivers `alive`/`snapshot_complete` to exactly one instance. The transport
  wires that broker signal to `active_state_change/2`, and the monitor holds
  control-plane authority only while active. While passive it neither issues
  recovery requests nor transitions producer status — a demoted instance can
  never observe the `snapshot_complete` for a request it issued, so issuing
  from standby burns recovery quota in a stall-reissue loop. Content-side
  observations (`message/2`, checkpoints) continue while passive.

  Demotion drops in-flight recovery correlation (the completion will go to the
  new owner); affected producers stay `:recovering` and are reissued on
  promotion. Producers that were `:up` heal through the normal alive-gap check
  after promotion, replaying from their checkpoint. Reports are best-effort and
  may repeat; they are not a fencing mechanism, so briefly-overlapping actives
  can duplicate a recovery request — duplicates are correlated by `request_id`
  and harmless beyond quota. Instances start active so the AMQP transport
  (shared queue, no ownership signal) is unaffected.
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
  @default_max_recovery_ms 60 * 60_000
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
  through the normal recovery lifecycle: the producer becomes `:recovering`,
  completion is correlated via `snapshot_complete`, and the
  stall guard reissues if it goes missing. Pass `full: true` to ignore the
  checkpoint and request a full snapshot. Refused while the producer is already
  recovering, including during a cooldown or retry delay, and with
  `{:error, :passive}` while this instance does not own the system
  subscription (see "Control-plane ownership").
  """
  @spec recover(GenServer.server(), integer(), keyword()) ::
          :ok | {:error, :already_recovering | :passive | :unknown_producer}
  def recover(server \\ __MODULE__, producer_id, opts) do
    GenServer.call(server, {:recover, producer_id, opts})
  end

  @doc """
  Observe a broker-reported ownership change for the system-subscription
  consumer. The Pulsar transport wires this as the system producer's
  `:active_state_callback`; `metadata` is the callback's map
  (`:active_state`, `:topic`, `:subscription`, `:consumer_pid`).

  Reports are idempotent: repeats of the current state are ignored.
  """
  def active_state_change(server \\ __MODULE__, metadata) do
    GenServer.cast(server, {:active_state, metadata})
  end

  @doc """
  Observe a pipeline connection session as `{namespace, token}`.

  Startup recovery waits until both the system and content connection namespaces
  have been observed, so replay starts only after both consumers are ready. After
  startup, a token change in any namespace recovers every producer to close the
  message-gap a reconnect leaves behind.
  """
  def observe_connection(server \\ __MODULE__, connection) do
    GenServer.cast(server, {:observe_connection, connection})
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
      monotonic_fun: Keyword.get(opts, :monotonic_fun, &monotonic_ms/0),
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
      recoveries: %{},
      last_recovery_at: %{},
      control_active: true
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

  def handle_call({:recover, _id, _opts}, _from, %{control_active: false} = state) do
    {:reply, {:error, :passive}, state}
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
        recover_after_connection_change(state)
      else
        %{state | connections: connections}
      end

    {:noreply, state}
  end

  def handle_cast({:active_state, %{active_state: active_state} = metadata}, state) do
    active? = active_state == :active

    if active? == state.control_active do
      {:noreply, state}
    else
      Logger.info(
        "UOF.SDK.ProducerMonitor: control plane #{if active?, do: "active", else: "passive"} " <>
          "(topic=#{inspect(metadata[:topic])} subscription=#{inspect(metadata[:subscription])})"
      )

      state = %{state | control_active: active?}
      state = if active?, do: reissue_parked_recoveries(state), else: park_recoveries(state)
      {:noreply, state}
    end
  end

  def handle_cast({:snapshot_complete, id, request_id}, state) do
    case Map.get(state.recoveries, id) do
      %{request_id: ^request_id} ->
        Logger.info("UOF.SDK.ProducerMonitor: producer #{id} recovery #{request_id} complete")
        state = %{state | recoveries: Map.delete(state.recoveries, id)}

        state =
          with_producer(state, id, fn producer ->
            commit_producer_transition(state, Producer.complete_recovery(producer, state.now_fun.()))
          end)

        {:noreply, state}

      _other ->
        # stale request_id, unknown producer, or another node's completion
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:tick, state) do
    state =
      if state.control_active do
        now = state.now_fun.()
        Enum.reduce(Map.values(state.producers), state, &check(&2, &1, now))
      else
        # A passive instance sees no system alives and only a partial content
        # view, so status transitions here would be noise; healing happens on
        # promotion when the checks resume against the then-stale timestamps.
        state
      end

    schedule_tick(state)
    {:noreply, state}
  end

  def handle_info({:stall, id, request_id}, state) do
    case Map.get(state.recoveries, id) do
      %{request_id: ^request_id, after_ts: after_ts, producer: producer} ->
        Logger.warning("UOF.SDK.ProducerMonitor: producer #{id} recovery #{request_id} stalled; reissuing")

        state = %{state | recoveries: Map.delete(state.recoveries, id)}
        {:noreply, issue(state, producer, after_ts)}

      _other ->
        {:noreply, state}
    end
  end

  def handle_info({:retry, producer, after_ts}, state) do
    if Map.has_key?(state.recoveries, producer.id) do
      {:noreply, state}
    else
      {:noreply, issue(state, producer, after_ts)}
    end
  end

  ## Health transitions -------------------------------------------------------

  defp on_alive(state, producer, gen_ts, subscribed?) do
    producer = Producer.observe_alive(producer, state.now_fun.())

    if producer.status != :recovering and (not subscribed? or producer.status == :down) do
      # An alive can race a just-received :passive report; a passive instance
      # must not trigger, same as before the startup connection gate opens.
      if startup_connection_recovery_pending?(state) or not state.control_active do
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
        commit_producer_transition(state, Producer.mark_delayed(producer, now - producer.last_message_timestamp))

      producer.status == :delayed and
          not processing_violation?(producer, now, state.max_processing_delay_ms) ->
        commit_producer_transition(state, Producer.mark_up(producer))

      producer.status == :resuming and producer.last_alive_at != nil and
        Connections.ready?(state.connections) and
          not processing_violation?(producer, now, state.max_processing_delay_ms) ->
        commit_producer_transition(state, Producer.mark_up(producer))

      true ->
        state
    end
  end

  defp trigger_recovery(state, before) do
    trigger_recovery(state, before, after_from_checkpoint(state, before))
  end

  defp trigger_recovery(state, before, after_ts) do
    after_ = Producer.start_recovery(before)
    state = commit_producer_transition(state, after_)
    initiate(state, after_, after_ts)
  end

  defp commit_producer_transition(state, after_) do
    state = put_producer(state, after_)
    state = persist_producer_state(state, after_)
    notify(state, after_)
    state
  end

  # Resuming safely requires both durable eligibility and a checkpoint. The
  # current-session heartbeat, connection, and catch-up gates live in check/3.
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

  ## Control-plane ownership ---------------------------------------------------

  # An in-flight request can never complete here once demoted — its
  # snapshot_complete goes to the new owner — so drop the correlation state
  # now. Pending stall/retry timers become no-ops (stall finds no matching
  # entry; retry hits the passive issue/3 clause).
  defp park_recoveries(%{recoveries: recoveries} = state) when map_size(recoveries) == 0, do: state

  defp park_recoveries(state) do
    Logger.info(
      "UOF.SDK.ProducerMonitor: parking in-flight recoveries for producers #{inspect(Map.keys(state.recoveries))}"
    )

    %{state | recoveries: %{}}
  end

  # after_ts is re-derived from the checkpoint rather than preserved from the
  # parked request; checkpoints don't advance while :recovering, so the replay
  # window is the same or wider. Producers left :up heal via the normal
  # alive-gap check on the next tick.
  defp reissue_parked_recoveries(state) do
    state.producers
    |> Map.values()
    |> Enum.filter(&(&1.status == :recovering and not Map.has_key?(state.recoveries, &1.id)))
    |> Enum.reduce(state, fn producer, state ->
      initiate(state, producer, after_from_checkpoint(state, producer))
    end)
  end

  defp startup_connection_recovery_pending?(state) do
    Connections.active?(state.connections) and not Connections.ready?(state.connections)
  end

  # A token change invalidates an in-flight request: it may have published
  # messages while the consumer was disconnected. A recovery already waiting
  # for its cooldown or an API retry needs no replacement because its request
  # will be issued after this reconnect and therefore covers the gap.
  defp recover_after_connection_change(state) do
    state.producers
    |> Map.values()
    |> Enum.reduce(state, fn producer, state ->
      case {producer.status, Map.has_key?(state.recoveries, producer.id)} do
        {:recovering, true} ->
          state = %{state | recoveries: Map.delete(state.recoveries, producer.id)}
          initiate(state, producer, after_from_checkpoint(state, producer))

        {:recovering, false} ->
          state

        {_status, _in_flight?} ->
          trigger_recovery(state, producer)
      end
    end)
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

  # Stall reissues and failure retries bypass this new-trigger cooldown because
  # their own timers already space them out.
  defp initiate(state, producer, after_ts) do
    case recovery_cooldown(state, producer) do
      0 -> issue(state, producer, after_ts)
      remaining -> defer(state, producer, after_ts, remaining)
    end
  end

  # Cooldowns are intentionally in-memory so a restart is never delayed by a
  # stale deadline.
  defp recovery_cooldown(state, producer) do
    case Map.get(state.last_recovery_at, producer.id) do
      nil -> 0
      last -> max(state.min_interval_ms - (state.monotonic_fun.() - last), 0)
    end
  end

  # The producer remains `:recovering`, but is not in-flight until this fires.
  defp defer(state, producer, after_ts, remaining) do
    Logger.info("UOF.SDK.ProducerMonitor: producer #{producer.id} recovery deferred for #{remaining}ms")

    Process.send_after(self(), {:retry, producer, after_ts}, remaining)
    state
  end

  # Backstop for retry/stall timers that fire after demotion: the producer
  # stays :recovering and is reissued from its checkpoint on promotion.
  defp issue(%{control_active: false} = state, producer, _after_ts) do
    Logger.info("UOF.SDK.ProducerMonitor: producer #{producer.id} recovery parked: control plane passive")
    state
  end

  defp issue(state, producer, after_ts) do
    state = %{
      state
      | last_recovery_at: Map.put(state.last_recovery_at, producer.id, state.monotonic_fun.())
    }

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
  defp monotonic_ms, do: System.monotonic_time(:millisecond)
end

defmodule UOF.SDK.ProducerMonitor do
  @moduledoc """
  Tracks producer health and orchestrates recovery — two concerns that are
  tightly coupled in the UOF protocol.

  At startup the monitor loads active producer descriptions from
  `UOF.API.Descriptions.producers/0`. It derives the recovery product from the
  description's `api_url` and keeps the advertised stateful recovery window.
  Startup fails if descriptions cannot be loaded; running without known
  producers would silently disable health tracking and recovery.

  ## Health monitoring

  `UOF.SDK.ProducerMonitor.Producer` is the per-producer state machine. It owns
  health observations, lifecycle transitions, its canonical recovery job, and
  the recovery cooldown. Delivery gaps move a producer from `:down` or `:up`
  to `:recovering`, and a matching recovery completion moves it to `:up`.
  Local processing lag moves `:up` to `:delayed` without recovery. A restart
  that can safely drain retained backlog starts at `:resuming` and becomes
  `:up` after current-session continuity is confirmed and processing catches
  up.

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

  Every required recovery has one canonical recovery job. A job without a
  `request_id` is pending (cooling down, retrying, or parked while passive); a
  job with a `request_id` is in flight. The producer's `:recovering` status is
  the public projection of that job, not a separate orchestration decision.
  Before any new job can issue HTTP, the monitor durably removes that producer
  from the resumable set so a crash cannot restart from stale synchronized
  state.

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
  from standby burns recovery quota in a stall-reissue loop. Content progress
  observations continue while passive.

  Demotion drops in-flight recovery correlation (the completion will go to the
  new owner); affected producers stay `:recovering` and are reissued on
  promotion. Producers that were `:up` heal through the normal alive-gap check
  after promotion, replaying from their checkpoint. Consume-session
  observations are ignored entirely while passive: the owner heals the shared
  feed, and a session change missed while passive leaves a stale committed
  baseline that re-detects the change — and recovers — on promotion. Reports
  are best-effort and may repeat; they are not a fencing mechanism, so
  briefly-overlapping actives can duplicate a recovery request — duplicates
  are correlated by `request_id` and harmless beyond quota.

  Pulsar monitors start in `{:failover, :passive}` ownership and act only after
  the broker's initial ownership report. AMQP monitors use `:always_active`
  ownership because no ownership signal exists there — a single AMQP consumer
  owns its own queues.
  """

  use GenServer

  alias UOF.SDK.ProducerMonitor.Connections
  alias UOF.SDK.ProducerMonitor.Producer
  alias UOF.SDK.ProducerMonitor.Store.Snapshot

  require Logger

  @default_inactivity_ms 20_000
  @default_max_processing_delay_ms 20_000
  @default_tick_ms 1_000
  @default_recovery_overlap_ms 5 * 60_000

  @type ownership :: :always_active | {:failover, :active | :passive}

  @type state :: %__MODULE__{
          producers: %{optional(integer()) => Producer.t()},
          snapshot: Snapshot.t(),
          connections: Connections.t(),
          ownership: ownership(),
          handler: module() | nil,
          now_fun: (-> integer()),
          monitor_store: module(),
          inactivity_ms: non_neg_integer(),
          max_processing_delay_ms: non_neg_integer(),
          tick_ms: pos_integer(),
          recovery_overlap_ms: non_neg_integer()
        }

  @enforce_keys [
    :producers,
    :snapshot,
    :connections,
    :ownership,
    :handler,
    :now_fun,
    :monitor_store,
    :inactivity_ms,
    :max_processing_delay_ms,
    :tick_ms,
    :recovery_overlap_ms
  ]

  defstruct @enforce_keys

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
      Map.new(load_producers(opts), fn producer ->
        producer = producer |> restore_producer(snapshot) |> Producer.configure_recovery(opts)
        {producer.id, producer}
      end)

    state = %__MODULE__{
      producers: producers,
      snapshot: snapshot,
      connections: Connections.new(snapshot.connection_tokens),
      ownership: Keyword.get(opts, :ownership, {:failover, :passive}),
      handler: Keyword.get(opts, :handler),
      now_fun: Keyword.get(opts, :now_fun, &now_ms/0),
      monitor_store: monitor_store,
      inactivity_ms: Keyword.get(opts, :inactivity_ms, @default_inactivity_ms),
      max_processing_delay_ms: Keyword.get(opts, :max_processing_delay_ms, @default_max_processing_delay_ms),
      tick_ms: Keyword.get(opts, :tick_ms, @default_tick_ms),
      recovery_overlap_ms: Keyword.get(opts, :recovery_overlap_ms, @default_recovery_overlap_ms)
    }

    schedule_tick(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:all_producers, _from, state) do
    result =
      state.producers
      |> Map.values()
      |> Enum.map(&Producer.public/1)
      |> Enum.sort_by(& &1.id)

    {:reply, result, state}
  end

  def handle_call({:get_producer, id}, _from, state) do
    result =
      case Map.fetch(state.producers, id) do
        {:ok, producer} -> {:ok, Producer.public(producer)}
        :error -> :error
      end

    {:reply, result, state}
  end

  def handle_call(:sync, _from, state), do: {:reply, :ok, state}

  def handle_call({:recover, id, opts}, _from, state) do
    cond do
      not owns_control?(state) ->
        {:reply, {:error, :passive}, state}

      producer_recovering?(state, id) ->
        {:reply, {:error, :already_recovering}, state}

      true ->
        recover_known_producer(state, id, opts)
    end
  end

  defp recover_known_producer(state, id, opts) do
    case Map.fetch(state.producers, id) do
      {:ok, producer} ->
        after_ts = if !opts[:full], do: after_from_checkpoint(state, producer)
        {:reply, :ok, trigger_recovery(state, producer, after_ts)}

      :error ->
        {:reply, {:error, :unknown_producer}, state}
    end
  end

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

  # A passive instance ignores session observations entirely: the owner heals
  # the shared feed, and the stale baseline this leaves behind is the
  # self-correction mechanism — on promotion (or a restart into ownership) the
  # current tokens are re-observed as never-seen and trigger the reconnect
  # recovery then, on the instance entitled to act.
  def handle_cast({:observe_connection, {namespace, token}}, state) do
    state = if owns_control?(state), do: observe_connection(state, namespace, token), else: state

    {:noreply, state}
  end

  def handle_cast({:active_state, _metadata}, %{ownership: :always_active} = state) do
    {:noreply, state}
  end

  def handle_cast({:active_state, %{active_state: active_state} = metadata}, state) do
    ownership = {:failover, if(active_state == :active, do: :active, else: :passive)}

    if ownership == state.ownership do
      {:noreply, state}
    else
      active? = ownership == {:failover, :active}

      Logger.info(
        "UOF.SDK.ProducerMonitor: control plane #{if active?, do: "active", else: "passive"} " <>
          "(topic=#{inspect(metadata[:topic])} subscription=#{inspect(metadata[:subscription])})"
      )

      state = %{state | ownership: ownership}
      state = if active?, do: resume_pending_recoveries(state), else: park_recoveries(state)
      {:noreply, state}
    end
  end

  def handle_cast({:snapshot_complete, id, request_id}, state) do
    {:noreply, complete_recovery(state, id, request_id)}
  end

  defp complete_recovery(state, id, request_id) do
    with_producer(state, id, fn producer ->
      case Producer.complete_recovery(producer, request_id, state.now_fun.()) do
        {:ok, producer} ->
          Logger.info("UOF.SDK.ProducerMonitor: producer #{id} recovery #{request_id} complete")
          commit_producer_transition(state, producer)

        :stale ->
          state
      end
    end)
  end

  @impl true
  def handle_info(:tick, state) do
    state =
      if owns_control?(state) do
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
    state =
      if owns_control?(state) do
        with_producer(state, id, fn producer ->
          put_producer(state, Producer.handle_stall(producer, request_id))
        end)
      else
        state
      end

    {:noreply, state}
  end

  # A timer that outlives a completed or superseded recovery no longer matches
  # the canonical job generation and must not issue.
  def handle_info({:retry, id, generation}, state) do
    state =
      if owns_control?(state) do
        with_producer(state, id, fn producer ->
          put_producer(state, Producer.handle_retry(producer, generation))
        end)
      else
        state
      end

    {:noreply, state}
  end

  ## Health transitions -------------------------------------------------------

  defp on_alive(state, producer, gen_ts, subscribed?) do
    result = Producer.observe_alive(producer, gen_ts, subscribed?, state.now_fun.())

    case result do
      {:recovery_needed, producer, _cause} ->
        # An alive can race a just-received passive report. Recovery also waits
        # until both pipeline connections are ready once startup observation
        # has begun.
        if startup_connection_recovery_pending?(state) or not owns_control?(state) do
          put_producer(state, producer)
        else
          trigger_recovery(state, producer)
        end

      {:checkpoint, producer, timestamp} ->
        state
        |> put_checkpoint(producer.id, timestamp)
        |> put_producer(producer)

      {:ok, producer} ->
        put_producer(state, producer)
    end
  end

  defp check(state, producer, now) do
    case Producer.check(
           producer,
           now,
           state.inactivity_ms,
           state.max_processing_delay_ms,
           Connections.ready?(state.connections)
         ) do
      {:recovery_needed, producer, _cause} ->
        if startup_connection_recovery_pending?(state), do: state, else: trigger_recovery(state, producer)

      {:transition, producer} ->
        commit_producer_transition(state, producer)

      :unchanged ->
        state
    end
  end

  defp trigger_recovery(state, before) do
    trigger_recovery(state, before, after_from_checkpoint(state, before))
  end

  defp trigger_recovery(state, before, after_ts) do
    producer = Producer.prepare_recovery(before, after_ts)

    # The durable safety transition must precede HTTP issuance. If the monitor
    # crashes after the request, restart will require another recovery instead
    # of resuming from stale synchronized state.
    state
    |> put_producer(producer)
    |> update_snapshot(&Snapshot.require_recovery(&1, before.id))
    |> notify_recovery(producer)
    |> initiate(producer)
  end

  defp commit_producer_transition(state, after_) do
    state = put_producer(state, after_)
    state = persist_producer_state(state, after_)
    notify(state, Producer.public(after_))
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

  defp load_producers(opts) do
    case Keyword.fetch(opts, :producers) do
      {:ok, producers} -> producers
      :error -> fetch_producers(opts)
    end
  end

  defp fetch_producers(opts) do
    fetcher = Keyword.get(opts, :producer_fetcher, &UOF.API.Descriptions.producers/0)

    response =
      try do
        fetcher.()
      rescue
        exception ->
          raise RuntimeError, "could not load UOF producers: #{Exception.message(exception)}"
      end

    case response do
      {:ok, %{producer: descriptions}} when is_list(descriptions) ->
        descriptions
        |> Enum.filter(& &1.active)
        |> Enum.map(&producer_from_description/1)

      other ->
        raise RuntimeError, "could not load UOF producers: #{inspect(other)}"
    end
  end

  defp producer_from_description(description) do
    %Producer{
      id: description.id,
      name: description.name,
      product: product_segment(description.api_url),
      recovery_window_minutes: description.stateful_recovery_window_in_minutes
    }
  end

  defp product_segment(nil), do: nil

  defp product_segment(api_url) do
    api_url |> to_string() |> String.trim_trailing("/") |> String.split("/") |> List.last()
  end

  defp notify(%{handler: nil}, _producer), do: :ok

  # Runs inside this GenServer: a raising handler crashes the monitor and is
  # healed by supervision — :rest_for_one restarts the pipelines with it, so
  # the re-subscribing consumers re-report failover ownership and
  # ownership converges (see the supervisor comment in UOF.SDK).
  defp notify(%{handler: handler}, producer), do: handler.handle_producer_status(producer)

  defp notify_recovery(state, producer) do
    notify(state, Producer.public(producer))
    state
  end

  ## Control-plane ownership ---------------------------------------------------

  defp park_recoveries(state) do
    in_flight = Enum.filter(state.producers, fn {_id, producer} -> recovery_in_flight?(producer) end)

    if in_flight == [] do
      state
    else
      ids = Enum.map(in_flight, &elem(&1, 0))
      Logger.info("UOF.SDK.ProducerMonitor: parking in-flight recoveries for producers #{inspect(ids)}")

      producers =
        Map.new(state.producers, fn {id, producer} ->
          {id, Producer.park_recovery(producer)}
        end)

      %{state | producers: producers}
    end
  end

  defp resume_pending_recoveries(state) do
    Enum.reduce(state.producers, state, fn {_id, producer}, state ->
      if Producer.recovery_pending?(producer), do: initiate(state, producer), else: state
    end)
  end

  defp startup_connection_recovery_pending?(state) do
    Connections.active?(state.connections) and not Connections.ready?(state.connections)
  end

  defp observe_connection(state, namespace, token) do
    case Connections.observe(state.connections, namespace, token) do
      {:recovery_needed, connections} ->
        commit_connection_change(state, Connections.commit(connections))

      {:not_ready, connections} ->
        %{state | connections: connections}

      {:unchanged, connections} ->
        %{state | connections: connections}
    end
  end

  defp commit_connection_change(state, connections) do
    state
    |> Map.put(:connections, connections)
    |> update_snapshot(&Snapshot.commit_connection_change(&1, connections.persisted, Map.keys(state.producers)))
    |> recover_after_connection_change()
  end

  # A token change invalidates an in-flight request: it may have published
  # messages while the consumer was disconnected. A recovery already waiting
  # for its cooldown or an API retry needs no replacement because its request
  # will be issued after this reconnect and therefore covers the gap.
  defp recover_after_connection_change(state) do
    state.producers
    |> Map.values()
    |> Enum.reduce(state, fn producer, state ->
      cond do
        Producer.recovery_pending?(producer) ->
          state

        Producer.recovering?(producer) ->
          producer = Producer.restart_recovery(producer, after_from_checkpoint(state, producer))
          put_producer(state, producer)

        true ->
          trigger_recovery(state, producer)
      end
    end)
  end

  # Runtime status is not stored. `:resuming` is reconstructed at startup from
  # the resumable-producer set and checkpoint.
  defp persist_producer_state(state, after_) do
    resumable? = after_.status in [:up, :delayed]

    if resumable? do
      update_snapshot(state, &Snapshot.mark_synchronized(&1, after_.id))
    else
      update_snapshot(state, &Snapshot.require_recovery(&1, after_.id))
    end
  end

  defp update_snapshot(state, update) do
    snapshot = update.(state.snapshot)

    if snapshot == state.snapshot do
      state
    else
      :ok = state.monitor_store.save(snapshot)
      %{state | snapshot: snapshot}
    end
  end

  ## Recovery -----------------------------------------------------------------

  defp initiate(state, producer) do
    put_producer(state, Producer.initiate_recovery(producer))
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
    update_snapshot(state, &Snapshot.advance_checkpoint(&1, id, timestamp))
  end

  defp clamp_to_window(timestamp, %Producer{recovery_window_minutes: nil}, _now), do: timestamp

  defp clamp_to_window(timestamp, %Producer{recovery_window_minutes: minutes}, now) do
    earliest = now - minutes * 60_000
    max(timestamp, earliest)
  end

  ## Helpers ------------------------------------------------------------------
  defp producer_recovering?(state, id) do
    case Map.fetch(state.producers, id) do
      {:ok, producer} -> Producer.recovering?(producer)
      :error -> false
    end
  end

  defp recovery_in_flight?(producer) do
    Producer.recovering?(producer) and not Producer.recovery_pending?(producer)
  end

  defp with_producer(state, id, fun) do
    case Map.fetch(state.producers, id) do
      {:ok, producer} -> fun.(producer)
      :error -> state
    end
  end

  defp owns_control?(%{ownership: :always_active}), do: true
  defp owns_control?(%{ownership: {:failover, :active}}), do: true
  defp owns_control?(%{ownership: {:failover, :passive}}), do: false

  defp schedule_tick(state), do: Process.send_after(self(), :tick, state.tick_ms)

  defp now_ms, do: System.system_time(:millisecond)
end

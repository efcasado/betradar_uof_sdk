defmodule UOF.SDK.ProducerMonitor do
  @moduledoc """
  Tracks producer health and orchestrates recovery: the SDK's control plane.

  Its inputs are the system pipeline's observations (`alive`,
  `snapshot_complete`, and the upstream consumer tag), the broker's ownership
  reports for the system subscription, and manual requests. Events only record
  facts and change recovery jobs. After every event and on every tick, the
  monitor issues what is due through `UOF.SDK.ProducerMonitor.Producer.advance/3`
  and publishes status changes, so every request passes the same gates: this
  instance owns the system subscription and the upstream session is confirmed.

  The monitor persists one snapshot (`UOF.SDK.ProducerMonitor.Snapshot`) per
  store, shared by a subscription's instances: the consumer tag and, per
  producer, a checkpoint and the recovery it owes. Only the active instance
  writes it, and promotion reloads it. Recovery intent is durable before any
  request is issued.

  Producer descriptions come from the UOF API, falling back to stored and then
  configured descriptions; an `alive` from an unknown producer triggers a
  rate-limited reload.

  The recovery guide (`docs/recovery.md`) describes the behaviour, and the
  architecture guide (`docs/architecture.md`) the design invariants this module
  must preserve.
  """

  use GenServer

  alias UOF.SDK.ProducerMonitor.Descriptions
  alias UOF.SDK.ProducerMonitor.Producer
  alias UOF.SDK.ProducerMonitor.Session
  alias UOF.SDK.ProducerMonitor.Snapshot
  alias UOF.SDK.ProducerStatus

  require Logger

  @default_inactivity_ms 20_000
  @default_tick_ms 1_000
  @default_descriptions_refresh_ms 60_000

  @type ownership :: :active | :passive

  @type state :: %__MODULE__{
          producers: %{optional(integer()) => Producer.t()},
          snapshot: Snapshot.t(),
          session: Session.t(),
          ownership: ownership(),
          consumer_pid: pid() | nil,
          consumer_ref: reference() | nil,
          handler: module() | nil,
          now_fun: (-> integer()),
          monitor_store: {module(), keyword()},
          readiness_deadline_at: integer() | nil,
          inactivity_ms: non_neg_integer(),
          tick_ms: pos_integer(),
          producer_opts: keyword(),
          descriptions_refresh_ms: pos_integer(),
          next_refresh_at: integer() | nil,
          refresh_scheduled?: boolean(),
          refresh_task: reference() | nil,
          published: %{optional(integer()) => ProducerStatus.status()}
        }

  @enforce_keys [
    :producers,
    :snapshot,
    :session,
    :ownership,
    :handler,
    :now_fun,
    :monitor_store,
    :readiness_deadline_at,
    ## Client API ---------------------------------------------------------------
    :inactivity_ms,
    :tick_ms
  ]

  defstruct @enforce_keys ++
              [
                consumer_pid: nil,
                consumer_ref: nil,
                producer_opts: [],
                descriptions_refresh_ms: @default_descriptions_refresh_ms,
                next_refresh_at: nil,
                refresh_scheduled?: false,
                refresh_task: nil,
                published: %{}
              ]

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
  stall guard reissues if it goes missing. If a persisted consume session has
  not been observed yet, the recovery requirement is recorded immediately and
  its HTTP request waits for the system pipeline's consumer tag. Pass `full: true` to ignore the
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

  ## Server ------------------------------------------------------------------

  @doc """
  Observe a broker-reported ownership change for the system-subscription
  consumer. `UOF.SDK.Pulsar` wires this as the system producer's
  `:active_state_callback`; `metadata` is the callback's map
  (`:active_state`, `:topic`, `:subscription`, `:consumer_pid`).

  Reports are idempotent for the same consumer. A replacement consumer is
  treated as a new ownership observation even if both report `:active`.
  """
  def active_state_change(server \\ __MODULE__, metadata) do
    GenServer.cast(server, {:active_state, metadata})
  end

  @doc """
  Observe the upstream consume session's consumer tag.

  After a restart, recovery waits until the current tag has been observed. A
  tag change recovers every producer to close the message gap a reconnect
  leaves behind.
  """
  def observe_consumer_tag(server \\ __MODULE__, consumer_tag) do
    GenServer.cast(server, {:observe_consumer_tag, consumer_tag})
  end

  @impl true
  def init(opts) do
    monitor_store = Keyword.get(opts, :monitor_store, {UOF.SDK.ProducerMonitor.Store.ETS, []})
    snapshot = load_snapshot(monitor_store)
    {descriptions, source} = Descriptions.load(opts, snapshot.descriptions)

    producers =
      Map.new(descriptions, fn description ->
        producer = description |> new_producer(opts) |> restore_producer(snapshot)
        {producer.id, producer}
      end)

    state = %__MODULE__{
      producers: producers,
      snapshot: snapshot,
      session: Session.new(snapshot.consumer_tag),
      ownership: Keyword.get(opts, :ownership, :passive),
      handler: Keyword.get(opts, :handler),
      now_fun: Keyword.get(opts, :now_fun, &now_ms/0),
      monitor_store: monitor_store,
      readiness_deadline_at: nil,
      inactivity_ms: Keyword.get(opts, :inactivity_ms, @default_inactivity_ms),
      tick_ms: Keyword.get(opts, :tick_ms, @default_tick_ms),
      producer_opts: opts,
      descriptions_refresh_ms: Keyword.get(opts, :descriptions_refresh_ms, @default_descriptions_refresh_ms)
    }

    state =
      case source do
        :api ->
          save_snapshot(state, %{snapshot | descriptions: descriptions})

        _fallback ->
          schedule_refresh(%{state | next_refresh_at: state.now_fun.() + state.descriptions_refresh_ms})
      end

    state = if owns_control?(state), do: start_active_monitoring(state), else: state

    state = %{
      state
      | published: Map.new(state.producers, fn {id, producer} -> {id, reported_status(producer, state)} end)
    }

    schedule_tick(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:all_producers, _from, state) do
    result =
      state.producers
      |> Map.values()
      |> Enum.map(&reported_producer(&1, state))
      |> Enum.sort_by(& &1.id)

    {:reply, result, state}
  end

  def handle_call({:get_producer, id}, _from, state) do
    result =
      case Map.fetch(state.producers, id) do
        {:ok, producer} -> {:ok, reported_producer(producer, state)}
        :error -> :error
      end

    {:reply, result, state}
  end

  def handle_call(:sync, _from, state), do: {:reply, :ok, state}

  def handle_call({:recover, id, opts}, _from, state) do
    case Map.fetch(state.producers, id) do
      :error ->
        {:reply, {:error, :unknown_producer}, state}

      {:ok, producer} ->
        cond do
          not owns_control?(state) ->
            {:reply, {:error, :passive}, state}

          Producer.recovering?(producer) ->
            {:reply, {:error, :already_recovering}, state}

          true ->
            kind = if opts[:full], do: :full, else: :incremental
            {:reply, :ok, state |> request_recovery(producer, kind) |> settle()}
        end
    end
  end

  @impl true
  # An alive from an unknown producer means it was activated after the
  # descriptions were loaded; it is picked up by the next reload.
  def handle_cast({:alive, id, gen_ts, subscribed?}, state) do
    state =
      case Map.fetch(state.producers, id) do
        {:ok, producer} -> on_alive(state, producer, gen_ts, subscribed?)
        :error -> schedule_refresh(state)
      end

    {:noreply, settle(state)}
  end

  # A passive instance ignores session observations entirely: the owner heals
  # the shared feed, and the stale baseline this leaves behind is the
  # self-correction mechanism — on promotion (or a restart into ownership) the
  # current consumer tag is re-observed as never-seen and triggers the reconnect
  # recovery then, on the instance entitled to act.
  def handle_cast({:observe_consumer_tag, tag}, state) do
    state = if owns_control?(state), do: track_consumer_tag(state, tag), else: state

    {:noreply, settle(state)}
  end

  def handle_cast({:active_state, %{active_state: active_state} = metadata}, state) do
    ownership = if active_state == :active, do: :active, else: :passive
    consumer_pid = Map.fetch!(metadata, :consumer_pid)

    state =
      cond do
        ownership == :active and not Process.alive?(consumer_pid) ->
          # A report from a worker that has already exited can arrive after
          # its replacement's report. Its DOWN cannot establish ownership.
          state

        ownership == :active and consumer_pid == state.consumer_pid ->
          state

        ownership == :active ->
          # A new worker may report before the old worker's DOWN is handled.
          # Re-enter through passive so promotion reloads the shared snapshot.
          state =
            if state.ownership == :active and state.consumer_pid != nil,
              do: change_ownership(state, :passive, metadata),
              else: state

          state
          |> monitor_consumer(consumer_pid)
          |> change_ownership(:active, metadata)

        state.consumer_pid != nil and consumer_pid != state.consumer_pid ->
          # A passive report from a superseded worker must not demote the
          # current owner.
          state

        true ->
          state
          |> clear_consumer_monitor()
          |> change_ownership(:passive, metadata)
      end

    {:noreply, settle(state)}
  end

  def handle_cast({:snapshot_complete, id, request_id}, state) do
    {:noreply, state |> complete_recovery(id, request_id) |> settle()}
  end

  defp monitor_consumer(state, consumer_pid) do
    state = clear_consumer_monitor(state)
    %{state | consumer_pid: consumer_pid, consumer_ref: Process.monitor(consumer_pid)}
  end

  defp clear_consumer_monitor(%{consumer_ref: nil} = state), do: state

  defp clear_consumer_monitor(state) do
    Process.demonitor(state.consumer_ref, [:flush])
    %{state | consumer_pid: nil, consumer_ref: nil}
  end

  defp change_ownership(%{ownership: ownership} = state, ownership, _metadata), do: state

  defp change_ownership(state, ownership, metadata) do
    active? = ownership == :active

    details =
      if metadata[:topic] do
        "topic=#{inspect(metadata[:topic])} subscription=#{inspect(metadata[:subscription])}"
      else
        "consumer_pid=#{inspect(metadata[:consumer_pid])} exited"
      end

    Logger.info(
      "UOF.SDK.ProducerMonitor: control plane #{if active?, do: "active", else: "passive"} " <>
        "(#{details})"
    )

    state = %{state | ownership: ownership}

    if active? do
      state
      |> reload_snapshot()
      |> start_active_monitoring()
    else
      state |> Map.put(:readiness_deadline_at, nil) |> park_recoveries()
    end
  end

  defp complete_recovery(state, id, request_id) do
    with_producer(state, id, fn producer ->
      case Producer.complete_recovery(producer, request_id, state.now_fun.()) do
        {:ok, producer} ->
          Logger.info("UOF.SDK.ProducerMonitor: producer #{id} recovery #{request_id} complete")
          synchronize(state, producer)

        # The producer stays :recovering and already requires recovery durably,
        # so the repeat needs no new transition or write.
        {:repeat, producer, resume_ts} ->
          put_producer(state, Producer.repeat_recovery(producer, preserve_full_recovery(state, producer, resume_ts)))

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
        ensure_session_readiness!(state, now)
        state.producers |> Map.values() |> Enum.reduce(state, &check(&2, &1, now)) |> settle()
      else
        # A passive instance sees no system alives, so status transitions here
        # would be noise; healing happens on
        # promotion when the checks resume against the then-stale timestamps.
        state
      end

    schedule_tick(state)
    {:noreply, state}
  end

  # The fetch runs in a task linked to the monitor, like recovery requests: an
  # unavailable API must not block heartbeats and status queries for the
  # request timeout. An exception from the fetcher still crashes the monitor.
  def handle_info(:refresh_descriptions, state) do
    opts = state.producer_opts
    task = Task.async(fn -> Descriptions.fetch(opts) end)

    {:noreply,
     %{
       state
       | refresh_scheduled?: false,
         refresh_task: task.ref,
         next_refresh_at: state.now_fun.() + state.descriptions_refresh_ms
     }}
  end

  def handle_info({:DOWN, ref, :process, consumer_pid, _reason}, %{consumer_ref: ref, consumer_pid: consumer_pid} = state) do
    state = clear_consumer_monitor(state)
    {:noreply, state |> change_ownership(:passive, %{consumer_pid: consumer_pid}) |> settle()}
  end

  def handle_info({ref, result}, %{refresh_task: ref} = state) do
    Process.demonitor(ref, [:flush])
    state = %{state | refresh_task: nil}

    state =
      case result do
        {:ok, descriptions} ->
          state |> apply_descriptions(descriptions) |> tap(fn _ -> Descriptions.emit_loaded(descriptions, :api) end)

        {:error, reason} ->
          Logger.warning("UOF.SDK.ProducerMonitor: reloading producer descriptions failed: #{inspect(reason)}")
          schedule_refresh(state)
      end

    {:noreply, state}
  end

  # A recovery request finished. A response for a job that was completed,
  # parked, or replaced meanwhile matches no producer and is dropped.
  def handle_info({ref, result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    state =
      case Enum.find(state.producers, fn {_id, producer} -> Producer.awaiting_response?(producer, ref) end) do
        ## Health transitions -------------------------------------------------------
        {_id, producer} -> put_producer(state, Producer.handle_response(producer, result))
        nil -> state
      end

    {:noreply, settle(state)}
  end

  defp on_alive(state, producer, gen_ts, subscribed?) do
    result = Producer.observe_alive(producer, gen_ts, subscribed?, state.now_fun.())
    emit_alive(result, subscribed?)

    case result do
      {:recovery_needed, producer, _cause} ->
        # Preserve explicit recovery intent even if this alive races demotion
        # or session startup. Only HTTP issuance depends on ownership and
        # readiness; the durable requirement itself must never be discarded.
        request_recovery(state, producer)

      {:checkpoint, producer, timestamp} ->
        state
        |> put_checkpoint(producer.id, timestamp)
        |> put_producer(producer)

      {:ok, producer} ->
        put_producer(state, producer)
    end
  end

  defp check(state, producer, now) do
    case Producer.check(producer, now, state.inactivity_ms, Session.ready?(state.session)) do
      {:recovery_needed, producer, :alive_timeout} ->
        request_recovery(state, producer)

      {:transition, producer} ->
        synchronize(state, producer)

      {:interrupted, producer} ->
        put_producer(state, producer)

      :unchanged ->
        state
    end
  end

  # Records that the producer owes a recovery and prepares its job. The owed
  # recovery is durable before reconcile/1 can issue the request, so a crash
  # after the request cannot resume from stale synchronized state. A full
  # recovery owed stays owed, so the job replays from the checkpoint only when
  # no full recovery is owed.
  defp request_recovery(state, producer, kind \\ :incremental) do
    state = owe_recovery(state, producer.id, kind)
    put_producer(state, Producer.prepare_recovery(producer, after_from_checkpoint(state, producer)))
  end

  # Only ever reached with the producer :up: a completed recovery or a resume.
  defp synchronize(state, producer) do
    state |> put_producer(producer) |> update_progress(producer.id, &%{&1 | pending_recovery: nil})
  end

  # Only a missing snapshot starts empty. One that cannot be decoded raises:
  # discarding it would lose the checkpoints that bound incremental recovery.
  defp load_snapshot({store, store_opts}) do
    case store.load(store_opts) do
      nil -> %Snapshot{}
      binary when is_binary(binary) -> Snapshot.decode!(binary)
    end
  end

  # Resuming safely requires both durable eligibility and a checkpoint. The
  # current-session heartbeat and session gates live in Producer.check/4.
  defp restore_producer(producer, snapshot) do
    if Snapshot.resumable?(snapshot, producer.id), do: %{producer | status: :resuming}, else: producer
  end

  defp new_producer(description, opts) do
    Producer |> struct(description) |> Producer.configure_recovery(opts)
  end

  # Reloaded descriptions update known producers and add new ones. A producer
  # added at runtime starts :down and recovers on its first alive. Producers no
  # longer listed keep their state.
  defp apply_descriptions(state, descriptions) do
    producers =
      Enum.reduce(descriptions, state.producers, fn description, producers ->
        case Map.fetch(producers, description.id) do
          {:ok, producer} -> Map.put(producers, description.id, struct(producer, description))
          :error -> Map.put(producers, description.id, new_producer(description, state.producer_opts))
        end
      end)

    save_snapshot(%{state | producers: producers}, %{state.snapshot | descriptions: descriptions})
  end

  # Reloads are rate limited to one per refresh interval, and one at a time: a
  # failed reload schedules the next itself.
  defp schedule_refresh(%{refresh_scheduled?: true} = state), do: state
  defp schedule_refresh(%{refresh_task: ref} = state) when is_reference(ref), do: state

  ## Control-plane ownership ---------------------------------------------------

  defp schedule_refresh(state) do
    delay = max((state.next_refresh_at || 0) - state.now_fun.(), 0)
    Process.send_after(self(), :refresh_descriptions, delay)
    %{state | refresh_scheduled?: true}
  end

  defp notify(state, %ProducerStatus{} = producer) do
    :telemetry.execute([:uof_sdk, :producer, :status], %{system_time: System.system_time()}, %{
      producer_id: producer.id,
      product: producer.product,
      status: producer.status
    })

    notify_handler(state, producer)
  end

  defp notify_handler(%{handler: nil}, _producer), do: :ok

  # Runs inside this GenServer: a raising handler crashes the monitor and is
  # healed by supervision — :rest_for_one restarts the pipelines with it, so
  # the re-subscribing consumers re-report failover ownership and
  # ownership converges (see the supervisor comment in UOF.SDK).
  defp notify_handler(%{handler: handler}, producer), do: handler.handle_producer_status(producer)

  defp park_recoveries(state) do
    in_flight = Enum.filter(state.producers, fn {_id, producer} -> Producer.recovery_in_flight?(producer) end)

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

  # The stored snapshot is the subscription's state, possibly written by the
  # previous owner while this instance was passive. Producers without a
  # recovery job are restored from it as on a restart. A retained job was
  # recorded while passive and not written, so it is made durable now; it
  # honours a full recovery the previous owner recorded. This instance keeps
  # the descriptions it loaded itself.
  defp reload_snapshot(state) do
    snapshot = %{load_snapshot(state.monitor_store) | descriptions: state.snapshot.descriptions}

    Enum.reduce(
      state.producers,
      %{state | snapshot: snapshot, session: Session.new(snapshot.consumer_tag)},
      fn {id, producer}, state ->
        if Producer.recovering?(producer) do
          state = owe_recovery(state, id, :incremental)
          full? = Snapshot.progress(state.snapshot, id).pending_recovery == :full
          if full?, do: put_producer(state, Producer.repeat_recovery(producer, nil)), else: state
        else
          restored = restore_producer(%{producer | status: :down, last_alive_at: nil, resuming_since: nil}, snapshot)
          put_producer(state, restored)
        end
      end
    )
  end

  # A resumable producer must hear a current heartbeat within one normal alive
  # interval after this instance acquires control, or the health check recovers
  # it. `resuming_since` starts that interval without counting as a heartbeat.
  # Session readiness gets the same interval; failure is process-fatal so
  # :rest_for_one restarts both pipelines instead of waiting forever.
  defp start_active_monitoring(state) do
    now = state.now_fun.()

    producers =
      Map.new(state.producers, fn {id, producer} ->
        producer =
          if producer.status == :resuming and is_nil(producer.resuming_since) do
            %{producer | resuming_since: now}
          else
            producer
          end

        {id, producer}
      end)

    arm_readiness_deadline(%{state | producers: producers}, now)
  end

  defp arm_readiness_deadline(%{readiness_deadline_at: nil} = state, now) do
    if owns_control?(state) and Session.awaiting_readiness?(state.session) do
      %{state | readiness_deadline_at: now + state.inactivity_ms}
    else
      state
    end
  end

  defp arm_readiness_deadline(state, _now), do: state

  defp ensure_session_readiness!(%{readiness_deadline_at: nil}, _now), do: :ok

  defp ensure_session_readiness!(%{readiness_deadline_at: deadline}, now) when now > deadline do
    raise RuntimeError, "producer monitor session readiness timed out; no consumer tag observed"
  end

  defp ensure_session_readiness!(_state, _now), do: :ok

  defp track_consumer_tag(state, tag) do
    was_ready? = Session.ready?(state.session)

    state =
      case Session.observe(state.session, tag) do
        {:recovery_needed, session} ->
          commit_session_change(state, Session.commit(session))

        {:unchanged, session} ->
          %{state | session: session}
      end

    # Readiness lets reconcile/1 issue jobs recorded before the first tag, even
    # when the tag matches the persisted baseline and reports `:unchanged`.
    if was_ready?, do: state, else: Map.put(state, :readiness_deadline_at, nil)
  end

  ## Recovery -----------------------------------------------------------------

  defp commit_session_change(state, session) do
    state
    |> save_snapshot(Snapshot.change_session(state.snapshot, session.persisted))
    |> Map.put(:session, session)
    |> recover_after_session_change()
  end

  # A consumer tag change invalidates an in-flight request: it may have published
  # messages while the consumer was disconnected. A recovery already waiting
  # for its cooldown or an API retry needs no replacement because its request
  # will be issued after this reconnect and therefore covers the gap.
  defp recover_after_session_change(state) do
    state.producers
    |> Map.values()
    |> Enum.reduce(state, fn producer, state ->
      cond do
        Producer.recovery_pending?(producer) ->
          state

        Producer.recovering?(producer) ->
          put_producer(state, Producer.repeat_recovery(producer, after_from_checkpoint(state, producer)))

        true ->
          request_recovery(state, producer)
      end
    end)
  end

  # Runtime status is not stored: `:resuming` is reconstructed at startup from
  # the checkpoint and the recovery still owed.
  defp owe_recovery(state, id, kind), do: update_progress(state, id, &Snapshot.Progress.owe(&1, kind))

  # Run after every event: issue what is due, then publish status changes.
  defp settle(state), do: state |> reconcile() |> publish_status_changes()

  # The only place recovery requests are issued. Every request therefore passes
  # the same gates: control-plane ownership and a confirmed upstream session.
  defp reconcile(state) do
    if may_issue?(state) do
      now = state.now_fun.()

      Enum.reduce(state.producers, state, fn {_id, producer}, state ->
        put_producer(state, Producer.advance(producer, now, state.inactivity_ms))
      end)
    else
      state
    end
  end

  # Status callbacks follow the reported status rather than individual
  # transitions. A producer added by a description reload is recorded without a
  # callback, like those loaded at startup.
  defp publish_status_changes(state) do
    Enum.reduce(state.producers, state, fn {id, producer}, state ->
      status = reported_status(producer, state)

      case Map.fetch(state.published, id) do
        {:ok, ^status} ->
          state

        {:ok, _previous} ->
          notify(state, reported_producer(producer, state))
          %{state | published: Map.put(state.published, id, status)}

        :error ->
          %{state | published: Map.put(state.published, id, status)}
      end
    end)
  end

  defp may_issue?(state), do: owns_control?(state) and not Session.awaiting_readiness?(state.session)

  defp after_from_checkpoint(state, producer) do
    progress = Snapshot.progress(state.snapshot, producer.id)
    if progress.pending_recovery == :full, do: nil, else: progress.checkpoint
  end

  defp preserve_full_recovery(state, producer, after_ts) do
    if Snapshot.progress(state.snapshot, producer.id).pending_recovery == :full, do: nil, else: after_ts
  end

  defp put_producer(state, %Producer{} = producer) do
    %{state | producers: Map.put(state.producers, producer.id, producer)}
  end

  # Checkpoints only move forward.
  defp put_checkpoint(state, id, timestamp) do
    update_progress(state, id, fn
      %{checkpoint: checkpoint} = progress when is_integer(checkpoint) and checkpoint >= timestamp -> progress
      progress -> %{progress | checkpoint: timestamp}
    end)
  end

  # Writes are skipped when nothing changed; otherwise the snapshot is durable
  # before the monitor acts on it.
  defp update_progress(state, id, fun) do
    progress = Snapshot.progress(state.snapshot, id)

    case fun.(progress) do
      ^progress -> state
      updated -> save_snapshot(state, Snapshot.put_progress(state.snapshot, id, updated))
    end
  end

  defp save_snapshot(%{snapshot: snapshot} = state, snapshot), do: state

  # Instances of one subscription may share a snapshot, which the active
  # instance owns. A passive instance's copy is stale, so it changes only in
  # memory; promotion reloads the stored one.
  #
  # Recovery intent from an unsubscribed alive that races demotion therefore
  # stays in the demoted instance. The new owner still learns of it: Betradar
  # keeps sending subscribed=0 until a recovery is requested, and a passive
  # instance never requests one, so the producer's next alive is unsubscribed
  # too.
  defp save_snapshot(%{monitor_store: {store, store_opts}} = state, snapshot) do
    if owns_control?(state), do: :ok = store.save(Snapshot.encode(snapshot), store_opts)
    %{state | snapshot: snapshot}
  end

  ## Helpers ------------------------------------------------------------------
  # Recovery state stays internal; a pending or in-flight job is reported as
  # `:recovering`. A pending job of a silent producer is reported as `:down`:
  # no recovery can start until the producer is heard again. A passive
  # instance hears no alives, so silence is only judged while active.
  defp reported_status(%Producer{} = producer, state) do
    cond do
      Producer.recovery_pending?(producer) and owns_control?(state) and
          Producer.silent?(producer, state.now_fun.(), state.inactivity_ms) ->
        :down

      Producer.recovering?(producer) ->
        :recovering

      true ->
        producer.status
    end
  end

  defp reported_producer(%Producer{} = producer, state) do
    %ProducerStatus{
      id: producer.id,
      name: producer.name,
      product: producer.product,
      recovery_window_minutes: producer.recovery_window_minutes,
      status: reported_status(producer, state),
      last_alive_at: producer.last_alive_at,
      alive_delay: producer.alive_delay
    }
  end

  defp emit_alive(result, subscribed?) do
    producer = elem(result, 1)

    if is_integer(producer.alive_delay) do
      :telemetry.execute([:uof_sdk, :producer, :alive], %{delay_ms: producer.alive_delay}, %{
        producer_id: producer.id,
        product: producer.product,
        subscribed: subscribed?
      })
    end
  end

  defp with_producer(state, id, fun) do
    case Map.fetch(state.producers, id) do
      {:ok, producer} -> fun.(producer)
      :error -> state
    end
  end

  defp owns_control?(%{ownership: ownership}), do: ownership == :active

  defp schedule_tick(state), do: Process.send_after(self(), :tick, state.tick_ms)

  defp now_ms, do: System.system_time(:millisecond)
end

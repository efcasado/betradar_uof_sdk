defmodule UOF.SDK.ProducerMonitor do
  @moduledoc """
  Tracks producer health and orchestrates recovery: the SDK's control plane.

  Its inputs are the system pipeline's observations (`alive`, with the
  upstream consumer tag it was delivered in, and `snapshot_complete`), the
  broker's ownership reports for the system subscription, and manual requests.
  Events only record facts on the producers
  (`UOF.SDK.ProducerMonitor.Producer`). After every event and on every tick,
  the monitor persists their durable progress, issues what is due through
  `UOF.SDK.ProducerMonitor.Producer.advance/3`, and publishes status changes.
  Every request is therefore issued only while this instance owns the system
  subscription, only for a producer heard in a session already checked, and
  only once its recovery intent is durable.

  The monitor persists one snapshot (`UOF.SDK.ProducerMonitor.Snapshot`) per
  store, shared by a subscription's instances: the consumer tag and, per
  producer, a checkpoint and the recovery it owes. Only the active instance
  writes it. Startup and promotion restore producers from it alike; a passive
  instance ignores system observations.

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

  @doc """
  Record an `alive` heartbeat and the consumer tag of the upstream session it
  was delivered in. `subscribed?` false means recovery is needed.

  The tag is checked before the alive is applied: a tag not seen before means
  the connector reconnected, so every producer owes a recovery to close the
  message gap a reconnect leaves behind.
  """
  def alive(server \\ __MODULE__, producer_id, gen_timestamp, subscribed?, consumer_tag) do
    GenServer.cast(server, {:alive, producer_id, gen_timestamp, subscribed?, consumer_tag})
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
  completion is correlated via `snapshot_complete`, and the stall guard
  reissues if it goes missing. The recovery requirement is recorded
  immediately; its HTTP request waits until the producer is heard and the
  cooldown has passed.
  Pass `full: true` to ignore the checkpoint and request a full snapshot.
  Refused while the producer already owes a recovery, and with
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
  consumer. `UOF.SDK.Pulsar` wires this as the system producer's
  `:active_state_callback`; `metadata` is the callback's map
  (`:active_state`, `:topic`, `:subscription`, `:consumer_pid`).

  Reports are idempotent for the same consumer. A replacement consumer is
  treated as a new ownership observation even if both report `:active`.
  """
  def active_state_change(server \\ __MODULE__, metadata) do
    GenServer.cast(server, {:active_state, metadata})
  end

  ## Server -------------------------------------------------------------------

  @impl true
  def init(opts) do
    monitor_store = Keyword.get(opts, :monitor_store, {UOF.SDK.ProducerMonitor.Store.ETS, []})
    snapshot = load_snapshot(monitor_store)
    {descriptions, source} = Descriptions.load(opts, snapshot.descriptions)

    producers =
      Map.new(descriptions, fn description ->
        producer = new_producer(description, opts)
        {producer.id, producer}
      end)

    state = %__MODULE__{
      producers: producers,
      snapshot: snapshot,
      session: Session.new(nil),
      ownership: Keyword.get(opts, :ownership, :passive),
      handler: Keyword.get(opts, :handler),
      now_fun: Keyword.get(opts, :now_fun, &now_ms/0),
      monitor_store: monitor_store,
      inactivity_ms: Keyword.get(opts, :inactivity_ms, @default_inactivity_ms),
      tick_ms: Keyword.get(opts, :tick_ms, @default_tick_ms),
      producer_opts: opts,
      descriptions_refresh_ms: Keyword.get(opts, :descriptions_refresh_ms, @default_descriptions_refresh_ms)
    }

    state = restore(state, snapshot)

    state =
      case source do
        :api ->
          save_snapshot(state, %{state.snapshot | descriptions: descriptions})

        _fallback ->
          schedule_refresh(%{state | next_refresh_at: state.now_fun.() + state.descriptions_refresh_ms})
      end

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
            {:reply, :ok, state |> put_producer(Producer.owe(producer, kind)) |> settle()}
        end
    end
  end

  @impl true
  # A passive instance ignores system observations: the owner heals the shared
  # feed, and promotion restores producers from the stored snapshot, which
  # replaces anything recorded here. An observation racing demotion is not
  # lost: the session baseline is the stored consumer tag, so a new tag is
  # re-detected on promotion, and Betradar keeps sending subscribed=0 until a
  # recovery is requested.
  def handle_cast(observation, %{ownership: :passive} = state) when elem(observation, 0) in [:alive, :snapshot_complete],
    do: {:noreply, state}

  # The session is checked before the alive is applied, so no producer is heard
  # in a session that has not been checked. An alive from an unknown producer
  # means it was activated after the descriptions were loaded; it is picked up
  # by the next reload.
  def handle_cast({:alive, id, gen_ts, subscribed?, consumer_tag}, state) do
    state = track_consumer_tag(state, consumer_tag)

    state =
      case Map.fetch(state.producers, id) do
        {:ok, producer} ->
          producer = Producer.observe_alive(producer, gen_ts, subscribed?, state.now_fun.())
          emit_alive(producer, subscribed?)
          put_producer(state, producer)

        :error ->
          schedule_refresh(state)
      end

    {:noreply, settle(state)}
  end

  def handle_cast({:snapshot_complete, id, request_id}, state) do
    now = state.now_fun.()
    {:noreply, state |> update_producer(id, &Producer.complete_recovery(&1, request_id, now)) |> settle()}
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
          # Re-enter through passive so promotion restores the shared snapshot.
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

  @impl true
  def handle_info(:tick, state) do
    state =
      if owns_control?(state) do
        now = state.now_fun.()

        producers =
          Map.new(state.producers, fn {id, producer} ->
            {id, Producer.check(producer, now, state.inactivity_ms)}
          end)

        settle(%{state | producers: producers})
      else
        # A passive instance observes nothing, so its producers' timestamps
        # are stale; promotion restores them from the snapshot.
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

  # A recovery request finished. A response for a request that was completed,
  # abandoned, or replaced meanwhile matches no producer and is dropped.
  def handle_info({ref, result}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    state =
      case Enum.find(state.producers, fn {_id, producer} -> Producer.awaiting_response?(producer, ref) end) do
        {_id, producer} -> put_producer(state, Producer.handle_response(producer, result))
        nil -> state
      end

    {:noreply, settle(state)}
  end

  ## Control-plane ownership --------------------------------------------------

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

    if active?, do: restore(state, load_snapshot(state.monitor_store)), else: state
  end

  # The stored snapshot is the subscription's state, possibly written by
  # another owner while this instance was passive. Startup and promotion
  # restore every producer from it alike: whatever this instance held in memory
  # is either recorded there or was settled by that owner. This instance keeps
  # the descriptions it loaded itself.
  defp restore(state, snapshot) do
    now = state.now_fun.()

    producers =
      Map.new(state.producers, fn {id, producer} ->
        {id, Producer.restore(producer, Snapshot.progress(snapshot, id), now)}
      end)

    %{
      state
      | producers: producers,
        snapshot: %{snapshot | descriptions: state.snapshot.descriptions},
        session: Session.new(snapshot.consumer_tag)
    }
  end

  ## Consume sessions ---------------------------------------------------------

  # An alive without a consumer tag carries no session information.
  defp track_consumer_tag(state, nil), do: state

  defp track_consumer_tag(state, tag) do
    case Session.observe(state.session, tag) do
      {:changed, session} -> recover_after_session_change(%{state | session: session})
      :unchanged -> state
    end
  end

  # A consumer tag change can leave a delivery gap, so every producer owes a
  # recovery. It also invalidates a request in flight: the replay may have been
  # published while the consumer was disconnected. A recovery still waiting to
  # be issued needs no replacement, as its request will cover the gap. The new
  # tag and the owed recoveries are saved in one write.
  defp recover_after_session_change(state) do
    producers =
      Map.new(state.producers, fn {id, producer} ->
        {id, producer |> Producer.drop_request() |> Producer.owe(:incremental)}
      end)

    %{state | producers: producers}
  end

  ## Recovery -----------------------------------------------------------------

  # Run after every event: persist, issue what is due, then publish status
  # changes.
  defp settle(state), do: state |> persist() |> reconcile() |> publish_status_changes()

  # The snapshot is a projection of the committed consumer tag and each
  # producer's durable progress. Saving it before reconcile/1 makes recovery
  # intent durable before any request, so a crash after the request cannot
  # resume from stale synchronized state.
  defp persist(state) do
    producers = Map.new(state.producers, fn {id, producer} -> {id, Producer.progress(producer)} end)
    save_snapshot(state, %{state.snapshot | consumer_tag: state.session.persisted, producers: producers})
  end

  # The only place recovery requests are issued, and only while this instance
  # owns the control plane. Producer.advance/3 issues only for a heard
  # producer, and every alive's consumer tag is checked before the alive is
  # applied, so no request precedes the check of its session.
  defp reconcile(state) do
    if owns_control?(state) do
      now = state.now_fun.()

      Enum.reduce(state.producers, state, fn {_id, producer}, state ->
        put_producer(state, Producer.advance(producer, now, state.inactivity_ms))
      end)
    else
      state
    end
  end

  ## Persistence --------------------------------------------------------------

  # Only a missing snapshot starts empty. One that cannot be decoded raises:
  # discarding it would lose the checkpoints that bound incremental recovery.
  defp load_snapshot({store, store_opts}) do
    case store.load(store_opts) do
      nil -> %Snapshot{}
      binary when is_binary(binary) -> Snapshot.decode!(binary)
    end
  end

  defp save_snapshot(%{snapshot: snapshot} = state, snapshot), do: state

  # Instances of one subscription may share a snapshot, which the active
  # instance owns. A passive instance's copy is stale, so it changes only in
  # memory; promotion restores from the stored one.
  defp save_snapshot(%{monitor_store: {store, store_opts}} = state, snapshot) do
    if owns_control?(state), do: :ok = store.save(Snapshot.encode(snapshot), store_opts)
    %{state | snapshot: snapshot}
  end

  ## Producer descriptions ----------------------------------------------------

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

  defp schedule_refresh(state) do
    delay = max((state.next_refresh_at || 0) - state.now_fun.(), 0)
    Process.send_after(self(), :refresh_descriptions, delay)
    %{state | refresh_scheduled?: true}
  end

  ## Status -------------------------------------------------------------------

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

  # A passive instance observes nothing, so silence is only judged while
  # active.
  defp reported_status(%Producer{} = producer, state) do
    silent? = owns_control?(state) and Producer.silent?(producer, state.now_fun.(), state.inactivity_ms)
    Producer.status(producer, silent?)
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

  defp emit_alive(%Producer{alive_delay: delay} = producer, subscribed?) when is_integer(delay) do
    :telemetry.execute([:uof_sdk, :producer, :alive], %{delay_ms: delay}, %{
      producer_id: producer.id,
      product: producer.product,
      subscribed: subscribed?
    })
  end

  defp emit_alive(%Producer{}, _subscribed?), do: :ok

  ## Helpers ------------------------------------------------------------------

  defp put_producer(state, %Producer{} = producer) do
    %{state | producers: Map.put(state.producers, producer.id, producer)}
  end

  defp update_producer(state, id, fun) do
    case Map.fetch(state.producers, id) do
      {:ok, producer} -> put_producer(state, fun.(producer))
      :error -> state
    end
  end

  defp owns_control?(%{ownership: ownership}), do: ownership == :active

  defp schedule_tick(state), do: Process.send_after(self(), :tick, state.tick_ms)

  defp now_ms, do: System.system_time(:millisecond)
end

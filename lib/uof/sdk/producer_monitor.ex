defmodule UOF.SDK.ProducerMonitor do
  @moduledoc """
  Coordinates producer health, recovery, persistence, and status notifications.

  The system pipeline supplies alives and completions; Pulsar reports ownership.
  Only the active owner writes the subscription's shared snapshot or sends
  recovery requests. Passive instances ignore system observations and reload
  the snapshot on promotion, using the same restore rules as startup.

  `UOF.SDK.ProducerMonitor.Producer` owns per-producer transitions;
  `UOF.SDK.ProducerMonitor.Requests` sends HTTP requests.
  See `docs/recovery.md` for behaviour and `docs/architecture.md` for invariants.
  """

  use GenServer

  alias UOF.SDK.ProducerMonitor.Descriptions
  alias UOF.SDK.ProducerMonitor.Producer
  alias UOF.SDK.ProducerMonitor.Requests
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
          requests: Requests.t(),
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
    :requests,
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
  Manually trigger a recovery for `producer_id` (e.g. from an admin UI), going
  through the normal recovery lifecycle: the producer becomes `:recovering`,
  completion is correlated via `snapshot_complete`, and the stall guard
  reissues if it goes missing. The recovery requirement is recorded
  immediately; its HTTP request waits until the producer is heard and the
  cooldown has passed.
  Pass `full: true` to ignore the checkpoint and request a full snapshot; it
  upgrades an incremental recovery the producer already owes, replacing any
  request in flight. Refused while the recovery the producer owes already
  covers the one requested, and with `{:error, :passive}` while this instance
  does not own the system subscription (see "Control-plane ownership").
  """
  @spec recover(GenServer.server(), integer(), keyword()) ::
          :ok | {:error, :already_recovering | :passive | :unknown_producer}
  def recover(server \\ __MODULE__, producer_id, opts) do
    GenServer.call(server, {:recover, producer_id, opts})
  end

  ## Wiring -------------------------------------------------------------------
  #
  # Called by the system pipeline and the Pulsar ownership callback, not by
  # applications.

  @doc false
  def alive(server \\ __MODULE__, producer_id, gen_timestamp, subscribed?, consumer_tag) do
    GenServer.cast(server, {:alive, producer_id, gen_timestamp, subscribed?, consumer_tag})
  end

  # A matching completion also proves system-message delivery, so it resets
  # the heartbeat timeout.
  @doc false
  def snapshot_complete(server \\ __MODULE__, producer_id, request_id) do
    GenServer.cast(server, {:snapshot_complete, producer_id, request_id})
  end

  # Pulsar's :active_state_callback supplies :active_state, :topic,
  # :subscription, and :consumer_pid.
  @doc false
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
        producer = new_producer(description)
        {producer.id, producer}
      end)

    state = %__MODULE__{
      producers: producers,
      requests: Requests.new(opts),
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
        kind = if opts[:full], do: :full, else: :incremental

        cond do
          not owns_control?(state) ->
            {:reply, {:error, :passive}, state}

          Producer.owes_recovery?(producer, kind) ->
            {:reply, {:error, :already_recovering}, state}

          true ->
            {:reply, :ok, state |> put_producer(Producer.owe(producer, kind)) |> settle()}
        end
    end
  end

  @impl true
  # Session changes are detected again after promotion, and unsubscribed
  # alives repeat until recovery is requested.
  def handle_cast(observation, %{ownership: :passive} = state) when elem(observation, 0) in [:alive, :snapshot_complete],
    do: {:noreply, state}

  # Check the upstream session before recording liveness or issuing recovery.
  def handle_cast({:alive, id, gen_ts, subscribed?, consumer_tag}, state) do
    state = track_consumer_tag(state, consumer_tag)
    now = state.now_fun.()

    state =
      case Map.fetch(state.producers, id) do
        {:ok, producer} ->
          producer =
            producer
            |> elapse(now, state)
            |> Producer.observe_alive(gen_ts, subscribed?, now, state.inactivity_ms)

          emit_alive(producer, subscribed?)
          put_producer(state, producer)

        :error ->
          schedule_refresh(state)
      end

    {:noreply, settle(state)}
  end

  def handle_cast({:snapshot_complete, id, request_id}, state) do
    now = state.now_fun.()
    complete = &Requests.complete(state.requests, elapse(&1, now, state), request_id, now)
    {:noreply, state |> update_producer(id, complete) |> settle()}
  end

  # Ignore reports from exited workers. A replacement must restore the shared
  # snapshot even if its predecessor's DOWN has not arrived.
  def handle_cast({:active_state, %{active_state: :active, consumer_pid: pid} = metadata}, state) do
    state =
      if pid == state.consumer_pid or not Process.alive?(pid),
        do: state,
        else: state |> demote_consumer(metadata) |> promote(pid, metadata)

    {:noreply, settle(state)}
  end

  # A passive report from a superseded worker must not demote the current
  # owner.
  def handle_cast({:active_state, %{consumer_pid: pid} = metadata}, state) do
    state = if state.consumer_pid in [nil, pid], do: demote(state, metadata), else: state
    {:noreply, settle(state)}
  end

  @impl true
  def handle_info(:tick, state) do
    state =
      if owns_control?(state) do
        now = state.now_fun.()
        producers = Map.new(state.producers, fn {id, producer} -> {id, elapse(producer, now, state)} end)
        settle(%{state | producers: producers})
      else
        state
      end

    schedule_tick(state)
    {:noreply, state}
  end

  # Fetch asynchronously so HTTP latency does not block the monitor.
  # Linked task failures still crash the monitor.
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

  # The active consumer may exit without a final passive report.
  def handle_info({:DOWN, ref, :process, consumer_pid, _reason}, %{consumer_ref: ref, consumer_pid: consumer_pid} = state) do
    {:noreply, state |> demote(%{consumer_pid: consumer_pid}) |> settle()}
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

  def handle_info({ref, {:recovery_response, id, request_id, result}}, state) when is_reference(ref) do
    Process.demonitor(ref, [:flush])
    respond = &Requests.handle_response(state.requests, &1, request_id, result)
    {:noreply, state |> update_producer(id, respond) |> settle()}
  end

  ## Control-plane ownership --------------------------------------------------

  defp promote(state, consumer_pid, metadata) do
    state = %{state | consumer_pid: consumer_pid, consumer_ref: Process.monitor(consumer_pid)}
    change_ownership(state, :active, metadata)
  end

  defp demote(state, metadata) do
    if state.consumer_ref, do: Process.demonitor(state.consumer_ref, [:flush])
    change_ownership(%{state | consumer_pid: nil, consumer_ref: nil}, :passive, metadata)
  end

  defp demote_consumer(%{consumer_pid: nil} = state, _metadata), do: state
  defp demote_consumer(state, metadata), do: demote(state, metadata)

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

  # Keep locally loaded descriptions when restoring shared subscription progress.
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

  defp track_consumer_tag(state, nil), do: state

  defp track_consumer_tag(state, tag) do
    case Session.observe(state.session, tag) do
      {:changed, session} -> recover_after_session_change(%{state | session: session})
      :unchanged -> state
    end
  end

  # A reconnect can lose replay messages too, so abandon in-flight requests.
  # settle/1 persists the new tag and recovery intent together.
  defp recover_after_session_change(state) do
    producers =
      Map.new(state.producers, fn {id, producer} ->
        {id, producer |> Producer.drop_request() |> Producer.owe(:incremental)}
      end)

    %{state | producers: producers}
  end

  ## Recovery -----------------------------------------------------------------

  defp settle(state), do: state |> persist() |> reconcile() |> publish_status_changes()

  # Persist recovery intent before HTTP issuance so a restart cannot resume
  # from stale synchronized state.
  defp persist(state) do
    producers = Map.new(state.producers, fn {id, producer} -> {id, Producer.progress(producer)} end)
    save_snapshot(state, %{state.snapshot | consumer_tag: state.session.persisted, producers: producers})
  end

  defp reconcile(state) do
    if owns_control?(state) do
      now = state.now_fun.()

      Enum.reduce(state.producers, state, fn {_id, producer}, state ->
        {requests, producer} = Requests.advance(state.requests, producer, now, state.inactivity_ms)
        put_producer(%{state | requests: requests}, producer)
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

  defp save_snapshot(%{monitor_store: {store, store_opts}} = state, snapshot) do
    if owns_control?(state), do: :ok = store.save(Snapshot.encode(snapshot), store_opts)
    %{state | snapshot: snapshot}
  end

  ## Producer descriptions ----------------------------------------------------

  defp new_producer(description), do: struct(Producer, description)

  # Producers omitted from a reload retain their existing state.
  defp apply_descriptions(state, descriptions) do
    producers =
      Enum.reduce(descriptions, state.producers, fn description, producers ->
        case Map.fetch(producers, description.id) do
          {:ok, producer} -> Map.put(producers, description.id, struct(producer, description))
          :error -> Map.put(producers, description.id, new_producer(description))
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

  # Newly discovered producers start without a status callback, as at startup.
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

  defp reported_status(%Producer{} = producer, state) do
    heard? =
      if owns_control?(state),
        do: Producer.heard?(producer, state.now_fun.(), state.inactivity_ms),
        else: is_integer(producer.last_alive_at)

    Producer.status(producer, heard?)
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

  # Runs synchronously; handler exceptions crash the monitor.
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

  # Check for silence before an alive or completion updates last_alive_at;
  # otherwise a gap ending between ticks can go unnoticed.
  defp elapse(%Producer{} = producer, now, state), do: Producer.check(producer, now, state.inactivity_ms)

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

defmodule UOF.SDK.ProducerMonitor do
  @moduledoc """
  Tracks producer health and orchestrates recovery — two concerns that are
  tightly coupled in the UOF protocol.

  At startup the monitor loads active producer descriptions from
  `UOF.API.Descriptions.producers/0`. It derives the recovery product from the
  description's `api_url` and keeps the advertised stateful recovery window.
  Loaded descriptions are stored in the snapshot. If the API fails, the
  monitor starts from the stored descriptions, or else from the configured
  `:producers`, and keeps retrying the API in the background. Startup fails
  only when no source has any: running without known producers would silently
  disable health tracking and recovery. An `alive` from a producer the monitor
  does not know also triggers a rate-limited reload, so producers activated
  after startup are picked up.

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

  Content lag is measured from the oldest content message still being
  handled, falling back to the newest one completed. Content processors report
  each message they start and finish (see `track_processing/4`), and this
  process keeps the in-flight message of every processor, so one processor
  stuck on an old message is not hidden by fresher messages completing
  elsewhere. Processors are monitored so one that dies mid-message is dropped.

  `:up` therefore means the feed confirmed synchronization (`snapshot_complete`
  or continuing heartbeats) and content processing is within
  `max_processing_delay_ms`. A recovery replay still being processed shows
  as `:delayed` once it lags beyond that threshold. With several instances on
  one Pulsar subscription, each monitor sees only its own share of content.

  ## Recovery orchestration

  Recovery checkpoints are owned here, not by the Broadway pipeline. Subscribed
  `alive` heartbeats advance the checkpoint only after the producer is already
  up. Incremental recovery subtracts `recovery_overlap_ms` from the stored
  checkpoint, intentionally replaying a bounded window to cover concurrent
  processing and distributed-consumer skew.

  A delivery gap marks the producer `:recovering`. The request is issued after
  the per-producer cooldown with a fresh `request_id` and a checkpoint-derived
  `after:` timestamp, clamped to the producer's recovery window. A heartbeat
  timeout records the recovery immediately but issues it only once the
  producer's alives resume: Betradar rejects recovery while a producer is
  silent, and failed requests count against its rate limit. Until then the
  producer is reported `:down`; the alive that releases the request reports
  it `:recovering`. Issuing emits
  `[:uof_sdk, :recovery, :initiated]` telemetry and starts the stall deadline.
  Failed requests are retried; stalled requests are reissued from the original
  timestamp. A matching `snapshot_complete` moves the producer to `:up`.

  Every required recovery has one canonical recovery job. A job without a
  `request_id` is pending (cooling down, retrying, or parked while passive); a
  job with a `request_id` is in flight. The producer's `:recovering` status is
  the public projection of that job, not a separate orchestration decision.
  Before any new job can issue HTTP, the monitor durably marks that producer as
  requiring recovery so a crash cannot restart from stale synchronized state.

  The recovery defaults follow the official SDK's recovery guidance and should
  be changed with care.

  ## Restart resume

  The monitor saves its state as one snapshot (`UOF.SDK.ProducerMonitor.Snapshot`):
  the committed consumer tags of both pipelines and, per producer, a checkpoint and
  whether it is synchronized with those sessions. A producer may resume only
  when it is synchronized and has a checkpoint. At startup that state is
  restored:

    * A resumable producer with a checkpoint starts as `:resuming`. It becomes
      `:up` after processing catches up, an `alive` is heard, and both pipeline
      connections are observed in the current process.
    * Persisted consumer tags are comparison baselines, not evidence that
      the current pipelines are ready. Recovery HTTP waits until both pipelines
      are observed. If that does not happen within one alive-inactivity
      interval, the monitor crashes so supervision restarts both pipelines.
      Once ready, a changed consumer tag is saved together with every producer marked
      unsynchronized, and recovery starts.
    * A resumable producer gets one alive-inactivity interval to provide a
      current heartbeat; silence then follows the normal recovery path instead
      of leaving it in `:resuming` forever.
    * `subscribed=0` records recovery immediately, even if it races demotion or
      connection startup; only the HTTP request waits. Producers not
      synchronized start `:down` and recover on their first subscribed
      `alive`.

  ## Control-plane ownership (multi-instance Pulsar)

  With the Pulsar transport, the system subscription is Failover: the broker
  delivers `alive`/`snapshot_complete` to exactly one instance. The transport
  wires that broker signal to `active_state_change/2`, and the monitor holds
  control-plane authority only while active. While passive it never issues
  recovery requests or runs periodic health transitions — a demoted instance
  cannot observe the `snapshot_complete` for a request it issued, so issuing
  from standby burns recovery quota in a stall-reissue loop. An explicit
  recovery-required alive racing demotion is nevertheless persisted and
  remains pending for promotion. Content progress observations continue while
  passive.

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
  alias UOF.SDK.ProducerMonitor.Snapshot
  alias UOF.SDK.ProducerStatus

  require Logger

  @default_inactivity_ms 20_000
  @default_max_processing_delay_ms 20_000
  @default_tick_ms 1_000
  @default_recovery_overlap_ms 5 * 60_000
  @default_descriptions_refresh_ms 60_000

  @type ownership :: :always_active | {:failover, :active | :passive}

  @type state :: %__MODULE__{
          producers: %{optional(integer()) => Producer.t()},
          snapshot: Snapshot.t(),
          connections: Connections.t(),
          ownership: ownership(),
          handler: module() | nil,
          now_fun: (-> integer()),
          monitor_store: {module(), keyword()},
          readiness_deadline_at: integer() | nil,
          inactivity_ms: non_neg_integer(),
          max_processing_delay_ms: non_neg_integer(),
          tick_ms: pos_integer(),
          recovery_overlap_ms: non_neg_integer(),
          in_flight: %{optional(pid()) => {integer(), integer()} | nil},
          producer_opts: keyword(),
          descriptions_refresh_ms: pos_integer(),
          next_refresh_at: integer() | nil,
          refresh_scheduled?: boolean(),
          refresh_task: reference() | nil
        }

  @enforce_keys [
    :producers,
    :snapshot,
    :connections,
    :ownership,
    :handler,
    :now_fun,
    :monitor_store,
    :readiness_deadline_at,
    :inactivity_ms,
    :max_processing_delay_ms,
    :tick_ms,
    :recovery_overlap_ms
  ]

  defstruct @enforce_keys ++
              [
                in_flight: %{},
                producer_opts: [],
                descriptions_refresh_ms: @default_descriptions_refresh_ms,
                next_refresh_at: nil,
                refresh_scheduled?: false,
                refresh_task: nil
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

  @doc "Record an `alive` heartbeat. `subscribed?` false means recovery is needed."
  def alive(server \\ __MODULE__, producer_id, gen_timestamp, subscribed?) do
    GenServer.cast(server, {:alive, producer_id, gen_timestamp, subscribed?})
  end

  @doc "Record a generation timestamp processed by the content pipeline."
  def message(server \\ __MODULE__, producer_id, gen_timestamp) do
    GenServer.cast(server, {:message, producer_id, gen_timestamp})
  end

  @doc """
  Run `fun` while the calling content processor is reported as handling a
  message generated at `gen_timestamp` for `producer_id`.

  Called by the content pipeline around the handler callback. Completion is
  reported when `fun` returns or raises, and records `gen_timestamp` as
  processed: a failed message no longer holds processing back.
  """
  @spec track_processing(GenServer.server(), integer(), integer(), (-> result)) :: result when result: term()
  def track_processing(server \\ __MODULE__, producer_id, gen_timestamp, fun) do
    GenServer.cast(server, {:processing, self(), producer_id, gen_timestamp})

    try do
      fun.()
    after
      GenServer.cast(server, {:processed, self()})
    end
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
  stall guard reissues if it goes missing. If a persisted connection session is
  still reconnecting, the recovery requirement is recorded immediately and its
  HTTP request waits for both pipelines. Pass `full: true` to ignore the
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
  Observe a pipeline connection as `{namespace, consumer_tag}`.

  Startup recovery waits until both the system and content connection namespaces
  have been observed, so replay starts only after both consumers are ready. After
  startup, a consumer tag change in any namespace recovers every producer to close the
  message-gap a reconnect leaves behind.
  """
  def observe_connection(server \\ __MODULE__, connection) do
    GenServer.cast(server, {:observe_connection, connection})
  end

  ## Server ------------------------------------------------------------------

  @impl true
  def init(opts) do
    monitor_store = Keyword.get(opts, :monitor_store, {UOF.SDK.ProducerMonitor.Store.ETS, []})
    snapshot = load_snapshot(monitor_store)
    {descriptions, source} = load_descriptions(opts, snapshot)

    producers =
      Map.new(descriptions, fn description ->
        producer = description |> new_producer(opts) |> restore_producer(snapshot)
        {producer.id, producer}
      end)

    state = %__MODULE__{
      producers: producers,
      snapshot: snapshot,
      connections: Connections.new(snapshot.consumer_tags),
      ownership: Keyword.get(opts, :ownership, {:failover, :passive}),
      handler: Keyword.get(opts, :handler),
      now_fun: Keyword.get(opts, :now_fun, &now_ms/0),
      monitor_store: monitor_store,
      readiness_deadline_at: nil,
      inactivity_ms: Keyword.get(opts, :inactivity_ms, @default_inactivity_ms),
      max_processing_delay_ms: Keyword.get(opts, :max_processing_delay_ms, @default_max_processing_delay_ms),
      tick_ms: Keyword.get(opts, :tick_ms, @default_tick_ms),
      recovery_overlap_ms: Keyword.get(opts, :recovery_overlap_ms, @default_recovery_overlap_ms),
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

    schedule_tick(state)
    {:ok, state}
  end

  @impl true
  def handle_call(:all_producers, _from, state) do
    result =
      state.producers
      |> Map.values()
      |> Enum.map(&reported_producer/1)
      |> Enum.sort_by(& &1.id)

    {:reply, result, state}
  end

  def handle_call({:get_producer, id}, _from, state) do
    result =
      case Map.fetch(state.producers, id) do
        {:ok, producer} -> {:ok, reported_producer(producer)}
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
            after_ts = if !opts[:full], do: after_from_checkpoint(state, producer)
            {:reply, :ok, request_recovery(state, producer, after_ts)}
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

    {:noreply, state}
  end

  def handle_cast({:message, id, gen_ts}, state) do
    {:noreply, observe_message(state, id, gen_ts)}
  end

  # A processor is monitored the first time it reports, so an entry cannot
  # outlive it. Between messages its entry is nil rather than removed.
  def handle_cast({:processing, pid, id, gen_ts}, state) do
    if not Map.has_key?(state.in_flight, pid), do: Process.monitor(pid)
    {:noreply, %{state | in_flight: Map.put(state.in_flight, pid, {id, gen_ts})}}
  end

  def handle_cast({:processed, pid}, state) do
    case Map.get(state.in_flight, pid) do
      {id, gen_ts} ->
        state = %{state | in_flight: Map.put(state.in_flight, pid, nil)}
        {:noreply, observe_message(state, id, gen_ts)}

      nil ->
        {:noreply, state}
    end
  end

  # A passive instance ignores session observations entirely: the owner heals
  # the shared feed, and the stale baseline this leaves behind is the
  # self-correction mechanism — on promotion (or a restart into ownership) the
  # current consumer tags are re-observed as never-seen and trigger the reconnect
  # recovery then, on the instance entitled to act.
  def handle_cast({:observe_connection, {namespace, tag}}, state) do
    state = if owns_control?(state), do: observe_connection(state, namespace, tag), else: state

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

      state =
        if active? do
          state |> start_active_monitoring() |> resume_pending_recoveries()
        else
          state |> Map.put(:readiness_deadline_at, nil) |> park_recoveries()
        end

      {:noreply, state}
    end
  end

  def handle_cast({:snapshot_complete, id, request_id}, state) do
    {:noreply, complete_recovery(state, id, request_id)}
  end

  defp complete_recovery(state, id, request_id) do
    with_producer(state, id, fn producer ->
      in_flight_ts = Map.get(oldest_in_flight(state), id)

      case Producer.complete_recovery(producer, request_id, state.now_fun.(), state.max_processing_delay_ms, in_flight_ts) do
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
        ensure_connection_readiness!(state, now)
        in_flight = oldest_in_flight(state)
        Enum.reduce(Map.values(state.producers), state, &check(&2, &1, now, Map.get(in_flight, &1.id)))
      else
        # A passive instance sees no system alives and only a partial content
        # view, so status transitions here would be noise; healing happens on
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
    task = Task.async(fn -> fetch_descriptions(opts) end)

    {:noreply,
     %{
       state
       | refresh_scheduled?: false,
         refresh_task: task.ref,
         next_refresh_at: state.now_fun.() + state.descriptions_refresh_ms
     }}
  end

  def handle_info({ref, result}, %{refresh_task: ref} = state) do
    Process.demonitor(ref, [:flush])
    state = %{state | refresh_task: nil}

    state =
      case result do
        {:ok, descriptions} ->
          state |> apply_descriptions(descriptions) |> tap(fn _ -> emit_loaded(descriptions, :api) end)

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
        {_id, producer} -> put_producer(state, Producer.handle_response(producer, result))
        nil -> state
      end

    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    {:noreply, %{state | in_flight: Map.delete(state.in_flight, pid)}}
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
        # Preserve explicit recovery intent even if this alive races demotion
        # or connection startup. Only HTTP issuance depends on ownership and
        # readiness; the durable requirement itself must never be discarded.
        request_recovery(state, producer)

      {:alive_resumed, producer} ->
        producer = Producer.release_recovery(producer, after_from_checkpoint(state, producer))
        state |> put_producer(producer) |> notify_recovery(producer) |> maybe_initiate(producer)

      {:checkpoint, producer, timestamp} ->
        state
        |> put_checkpoint(producer.id, timestamp)
        |> put_producer(producer)

      {:ok, producer} ->
        put_producer(state, producer)
    end
  end

  defp check(state, producer, now, in_flight_ts) do
    case Producer.check(
           producer,
           now,
           state.inactivity_ms,
           state.max_processing_delay_ms,
           Connections.ready?(state.connections),
           in_flight_ts
         ) do
      {:recovery_needed, producer, :alive_timeout} ->
        {state, _producer} = prepare_recovery(state, producer, after_from_checkpoint(state, producer), true)
        state

      {:transition, producer} ->
        commit_producer_transition(state, producer)

      :unchanged ->
        state
    end
  end

  defp request_recovery(state, before) do
    request_recovery(state, before, after_from_checkpoint(state, before))
  end

  defp request_recovery(state, before, after_ts) do
    {state, producer} = prepare_recovery(state, before, after_ts)
    maybe_initiate(state, producer)
  end

  defp prepare_recovery(state, before, after_ts, awaiting_alive \\ false) do
    producer = Producer.prepare_recovery(before, after_ts, awaiting_alive)

    # The durable safety transition must precede HTTP issuance. If the monitor
    # crashes after the request, restart will require another recovery instead
    # of resuming from stale synchronized state.
    state =
      state
      |> put_producer(producer)
      |> require_recovery(before.id)
      |> notify_recovery(producer)

    {state, producer}
  end

  defp commit_producer_transition(state, after_) do
    state = put_producer(state, after_)
    state = persist_producer_progress(state, after_)
    notify(state, reported_producer(after_))
    state
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
  # current-session heartbeat, connection, and content catch-up gates live in
  # Producer.check/6. The checkpoint is not content evidence: catching up
  # requires content processed in this session.
  defp restore_producer(producer, snapshot) do
    if Snapshot.resumable?(snapshot, producer.id), do: %{producer | status: :resuming}, else: producer
  end

  # The API is authoritative. When it fails, the descriptions stored by a
  # previous run, or else the configured ones, let the monitor start; it keeps
  # retrying the API. With no source at all, startup fails: running without
  # known producers would silently disable health tracking and recovery.
  defp load_descriptions(opts, snapshot) do
    case fetch_descriptions(opts) do
      {:ok, descriptions} ->
        emit_loaded(descriptions, :api)
        {descriptions, :api}

      {:error, reason} ->
        fall_back(reason, snapshot.descriptions, Keyword.get(opts, :producers, []))
    end
  end

  defp fall_back(reason, [_ | _] = stored, _configured), do: degraded(reason, stored, :snapshot)
  defp fall_back(reason, [], [_ | _] = configured), do: degraded(reason, configured, :config)

  defp fall_back(reason, [], []) do
    raise RuntimeError,
          "could not load UOF producers: #{inspect(reason)}; no stored or configured producer descriptions"
  end

  defp degraded(reason, descriptions, source) do
    Logger.warning(
      "UOF.SDK.ProducerMonitor: could not load producer descriptions from the UOF API (#{inspect(reason)}); " <>
        "starting from #{source} descriptions and retrying"
    )

    emit_loaded(descriptions, source)
    {descriptions, source}
  end

  defp fetch_descriptions(opts) do
    fetcher = Keyword.get(opts, :producer_fetcher, &UOF.API.Descriptions.producers/0)

    case fetcher.() do
      {:ok, %{producer: descriptions}} when is_list(descriptions) ->
        {:ok, descriptions |> Enum.filter(& &1.active) |> Enum.map(&description/1)}

      {:error, reason} ->
        {:error, reason}

      other ->
        {:error, {:unexpected_response, other}}
    end
  end

  defp description(api_description) do
    %{
      id: api_description.id,
      name: api_description.name,
      product: product_segment(api_description.api_url),
      recovery_window_minutes: api_description.stateful_recovery_window_in_minutes
    }
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

  defp schedule_refresh(state) do
    delay = max((state.next_refresh_at || 0) - state.now_fun.(), 0)
    Process.send_after(self(), :refresh_descriptions, delay)
    %{state | refresh_scheduled?: true}
  end

  defp emit_loaded(descriptions, source) do
    :telemetry.execute([:uof_sdk, :producers, :loaded], %{count: length(descriptions)}, %{source: source})
  end

  defp product_segment(nil), do: nil

  defp product_segment(api_url) do
    api_url |> to_string() |> String.trim_trailing("/") |> String.split("/") |> List.last()
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

  defp notify_recovery(state, producer) do
    notify(state, reported_producer(producer))
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

  # A job awaiting an alive stays parked: the producer's next alive releases it.
  defp resume_pending_recoveries(state) do
    Enum.reduce(state.producers, state, fn {_id, producer}, state ->
      if Producer.recovery_pending?(producer) and not Producer.awaiting_alive?(producer),
        do: maybe_initiate(state, producer),
        else: state
    end)
  end

  # A resumable producer must hear a current heartbeat within one normal alive
  # interval after this instance acquires control, or the health check recovers
  # it. `resuming_since` starts that interval without counting as a heartbeat.
  # Connection readiness gets the same interval; failure is process-fatal so
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

  defp arm_readiness_deadline(state, now \\ nil)

  defp arm_readiness_deadline(%{readiness_deadline_at: nil} = state, now) do
    if owns_control?(state) and Connections.awaiting_readiness?(state.connections) do
      now = now || state.now_fun.()
      %{state | readiness_deadline_at: now + state.inactivity_ms}
    else
      state
    end
  end

  defp arm_readiness_deadline(state, _now), do: state

  defp ensure_connection_readiness!(%{readiness_deadline_at: nil}, _now), do: :ok

  defp ensure_connection_readiness!(%{readiness_deadline_at: deadline} = state, now) when now > deadline do
    missing = Connections.missing_namespaces(state.connections)

    raise RuntimeError,
          "producer monitor connection readiness timed out; missing namespaces: #{inspect(missing)}"
  end

  defp ensure_connection_readiness!(_state, _now), do: :ok

  defp observe_connection(state, namespace, tag) do
    was_ready? = Connections.ready?(state.connections)

    state =
      case Connections.observe(state.connections, namespace, tag) do
        {:recovery_needed, connections} ->
          commit_session_change(state, Connections.commit(connections))

        {:not_ready, connections} ->
          %{state | connections: connections}

        {:unchanged, connections} ->
          %{state | connections: connections}
      end

    # A recovery-required alive may have arrived after the first connection.
    # Readiness itself must release that pending job even when the second tag
    # matches the persisted baseline and therefore reports `:unchanged`.
    if not was_ready? and Connections.ready?(state.connections) do
      state
      |> Map.put(:readiness_deadline_at, nil)
      |> resume_pending_recoveries()
    else
      arm_readiness_deadline(state)
    end
  end

  defp commit_session_change(state, connections) do
    state
    |> save_snapshot(Snapshot.change_session(state.snapshot, connections.persisted))
    |> Map.put(:connections, connections)
    |> recover_after_connection_change()
  end

  # A consumer tag change invalidates an in-flight request: it may have published
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
          request_recovery(state, producer)
      end
    end)
  end

  # Runtime status is not stored. `:resuming` is reconstructed at startup from
  # the producer checkpoint and its synchronization flag.
  defp persist_producer_progress(state, after_) do
    synchronized? = after_.status in [:up, :delayed]
    update_progress(state, after_.id, &%{&1 | synchronized: synchronized?})
  end

  defp require_recovery(state, id) do
    update_progress(state, id, &%{&1 | synchronized: false})
  end

  ## Recovery -----------------------------------------------------------------

  defp initiate(state, producer) do
    put_producer(state, Producer.initiate_recovery(producer))
  end

  defp maybe_initiate(state, producer) do
    if owns_control?(state) and not Connections.awaiting_readiness?(state.connections) do
      initiate(state, producer)
    else
      state
    end
  end

  defp after_from_checkpoint(state, producer) do
    case Snapshot.progress(state.snapshot, producer.id).checkpoint do
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

  defp save_snapshot(%{monitor_store: {store, store_opts}} = state, snapshot) do
    :ok = store.save(Snapshot.encode(snapshot), store_opts)
    %{state | snapshot: snapshot}
  end

  defp clamp_to_window(timestamp, %Producer{recovery_window_minutes: nil}, _now), do: timestamp

  defp clamp_to_window(timestamp, %Producer{recovery_window_minutes: minutes}, now) do
    earliest = now - minutes * 60_000
    max(timestamp, earliest)
  end

  ## Helpers ------------------------------------------------------------------
  # Recovery state stays internal; a pending or in-flight job is reported as
  # `:recovering`. A job awaiting the producer's alives is reported as `:down`:
  # the producer is silent and no recovery can start until it is heard again.
  defp reported_producer(%Producer{} = producer) do
    recovering? = Producer.recovering?(producer)

    status =
      cond do
        Producer.awaiting_alive?(producer) -> :down
        recovering? -> :recovering
        true -> producer.status
      end

    %ProducerStatus{
      id: producer.id,
      name: producer.name,
      product: producer.product,
      recovery_window_minutes: producer.recovery_window_minutes,
      status: status,
      last_alive_at: producer.last_alive_at,
      last_message_timestamp: producer.last_message_timestamp,
      processing_queue_delay: if(!recovering?, do: producer.processing_queue_delay)
    }
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

  # Oldest in-flight content timestamp per producer.
  defp oldest_in_flight(state) do
    Enum.reduce(state.in_flight, %{}, fn
      {_pid, {id, timestamp}}, oldest -> Map.update(oldest, id, timestamp, &min(&1, timestamp))
      {_pid, nil}, oldest -> oldest
    end)
  end

  defp observe_message(state, id, gen_ts) do
    with_producer(state, id, fn p -> put_producer(state, Producer.observe_message(p, gen_ts)) end)
  end

  defp schedule_tick(state), do: Process.send_after(self(), :tick, state.tick_ms)

  defp now_ms, do: System.system_time(:millisecond)
end

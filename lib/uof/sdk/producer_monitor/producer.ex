defmodule UOF.SDK.ProducerMonitor.Producer do
  @moduledoc """
  State machine for one Betradar producer.

  The struct keeps the producer description, health observations, and its
  complete recovery state together: static request configuration, cooldown
  history, and an optional pending or in-flight job. `UOF.SDK.ProducerMonitor`
  supplies global inputs such as connection readiness and control-plane
  ownership. This module starts per-producer recovery HTTP requests in tasks
  linked to the monitor and handles their responses, and owns timers,
  logging, and telemetry; the monitor retains event routing, durable
  persistence, and callbacks.

  `status` holds health state only. `UOF.SDK.ProducerMonitor` reports a
  producer with a recovery job as `:recovering` in `UOF.SDK.ProducerStatus`,
  or as `:down` while the job awaits the producer's alives.

  ## Recovery sequencing

  Preparing a recovery and issuing its HTTP request are deliberately separate.
  `UOF.SDK.ProducerMonitor` first calls `prepare_recovery/2`, durably marks the
  producer as requiring recovery, and publishes the `:recovering` transition.
  Only then does it call `initiate_recovery/1`. This ordering guarantees that a
  crash after issuing the request cannot restart from stale synchronized state.

  A heartbeat timeout prepares its job awaiting an alive. Recovery requests
  issued while the producer is silent fail and count against Betradar's rate
  limit, so `observe_alive/4` reports the next alive and the monitor releases
  the job then, recomputing its replay timestamp.

  A reconnect is different: the session change is saved before
  the producers are visited, so `restart_recovery/2` can replace an in-flight
  job and initiate it immediately. Failed requests, stalls, and ownership
  demotion all return a job to pending state, but their callers intentionally
  choose different reissue times.
  """

  alias UOF.Schemas.Common.Response

  require Logger

  @default_min_interval_ms 30_000
  @default_max_recovery_ms 60 * 60_000
  @max_request_id 1_000_000

  @type status :: :down | :up | :delayed | :resuming

  @type recovery_job :: %{
          after_ts: integer() | nil,
          request_id: integer() | nil,
          generation: reference(),
          awaiting_alive: boolean(),
          task: reference() | nil
        }

  @type recovery_state :: %{
          job: recovery_job() | nil,
          last_issued_at: integer() | nil,
          request: (term(), keyword() -> term()),
          next_request_id: pos_integer(),
          monotonic_fun: (-> integer()),
          node_id: integer() | nil,
          min_interval_ms: non_neg_integer(),
          max_recovery_ms: pos_integer()
        }

  @type t :: %__MODULE__{
          id: integer(),
          product: String.t() | nil,
          name: String.t() | nil,
          recovery_window_minutes: integer() | nil,
          status: status(),
          last_alive_at: integer() | nil,
          resuming_since: integer() | nil,
          last_message_timestamp: integer() | nil,
          processing_queue_delay: integer() | nil,
          recovery: recovery_state() | nil
        }

  @type alive_result ::
          {:ok, t()}
          | {:alive_resumed, t()}
          | {:checkpoint, t(), integer()}
          | {:recovery_needed, t(), :initial_sync | :unsubscribed}

  @type check_result ::
          :unchanged
          | {:transition, t()}
          | {:recovery_needed, t(), :alive_timeout}

  defstruct [
    :id,
    :product,
    :name,
    :recovery_window_minutes,
    :last_alive_at,
    :resuming_since,
    :last_message_timestamp,
    :processing_queue_delay,
    :recovery,
    status: :down
  ]

  @doc false
  @spec configure_recovery(t(), keyword()) :: t()
  def configure_recovery(%__MODULE__{} = producer, opts) do
    recovery = %{
      job: nil,
      last_issued_at: nil,
      request: Keyword.get(opts, :recover_fun, &UOF.API.Recovery.recover/2),
      next_request_id: Keyword.get_lazy(opts, :request_id_start, fn -> :rand.uniform(@max_request_id) end),
      monotonic_fun: Keyword.get(opts, :monotonic_fun, &monotonic_ms/0),
      node_id: Keyword.get(opts, :node_id),
      min_interval_ms: Keyword.get(opts, :min_interval_ms, @default_min_interval_ms),
      max_recovery_ms: Keyword.get(opts, :max_recovery_ms, @default_max_recovery_ms)
    }

    %{producer | recovery: recovery}
  end

  @doc false
  @spec observe_alive(t(), integer() | nil, boolean(), integer()) :: alive_result()
  def observe_alive(%__MODULE__{} = producer, gen_timestamp, subscribed?, now) do
    observed = %{producer | last_alive_at: now}

    cond do
      awaiting_alive?(producer) ->
        {:alive_resumed, observed}

      recovering?(producer) ->
        {:ok, observed}

      not subscribed? ->
        {:recovery_needed, observed, :unsubscribed}

      producer.status == :down ->
        {:recovery_needed, observed, :initial_sync}

      producer.status == :up and is_integer(gen_timestamp) ->
        {:checkpoint, observed, gen_timestamp}

      true ->
        {:ok, observed}
    end
  end

  @doc false
  @spec observe_message(t(), integer() | nil) :: t()
  def observe_message(%__MODULE__{} = producer, timestamp) do
    %{producer | last_message_timestamp: max_timestamp(producer.last_message_timestamp, timestamp)}
  end

  @doc false
  @spec check(t(), integer(), non_neg_integer(), non_neg_integer(), boolean(), integer() | nil) :: check_result()
  def check(producer, now, inactivity_ms, max_processing_delay_ms, connections_ready?, in_flight_ts) do
    freshness = content_freshness(producer, in_flight_ts)

    cond do
      recovering?(producer) ->
        :unchanged

      alive_violation?(producer, now, inactivity_ms) ->
        {:recovery_needed, producer, :alive_timeout}

      producer.status == :up and processing_violation?(freshness, now, max_processing_delay_ms) ->
        {:transition, mark_delayed(producer, now - freshness)}

      producer.status == :delayed and
          not processing_violation?(freshness, now, max_processing_delay_ms) ->
        {:transition, mark_up(producer)}

      producer.status == :resuming and producer.last_alive_at != nil and connections_ready? and
        freshness != nil and not processing_violation?(freshness, now, max_processing_delay_ms) ->
        {:transition, mark_up(producer)}

      true ->
        :unchanged
    end
  end

  @doc false
  @spec prepare_recovery(t(), integer() | nil, boolean()) :: t()
  def prepare_recovery(%__MODULE__{recovery: recovery} = producer, after_ts, awaiting_alive \\ false) do
    job = %{after_ts: after_ts, request_id: nil, generation: make_ref(), awaiting_alive: awaiting_alive, task: nil}
    %{producer | recovery: %{recovery | job: job}}
  end

  # The checkpoint is frozen while recovering, but the recovery window moves:
  # the caller recomputes the replay timestamp for the moment of issue.
  @doc false
  @spec release_recovery(t(), integer() | nil) :: t()
  def release_recovery(%__MODULE__{recovery: %{job: job} = recovery} = producer, after_ts) do
    %{producer | recovery: %{recovery | job: %{job | after_ts: after_ts, awaiting_alive: false}}}
  end

  # Connection invalidation is already durable when this is called, so the
  # replacement may proceed directly through cooldown and HTTP issuance.
  @doc false
  @spec restart_recovery(t(), integer() | nil) :: t()
  def restart_recovery(%__MODULE__{recovery: recovery} = producer, after_ts) do
    job = %{recovery.job | after_ts: after_ts, request_id: nil, generation: make_ref(), task: nil}
    producer = %{producer | recovery: %{recovery | job: job}}
    initiate_recovery(producer)
  end

  # This transition intentionally performs no I/O: stalls reissue immediately,
  # request failures wait for the retry interval, and demotion waits for
  # promotion.
  @spec mark_recovery_pending(t()) :: t()
  defp mark_recovery_pending(%__MODULE__{recovery: recovery} = producer) do
    job = %{recovery.job | request_id: nil, generation: make_ref(), task: nil}
    %{producer | recovery: %{recovery | job: job}}
  end

  @doc false
  @spec complete_recovery(t(), integer(), integer(), non_neg_integer(), integer() | nil) :: {:ok, t()} | :stale
  # Completion applies the same content-lag check as a health tick: content
  # already known to lag (e.g. a stuck handler) completes the recovery as
  # :delayed rather than reporting :up for one tick.
  def complete_recovery(
        %__MODULE__{recovery: %{job: %{request_id: request_id}} = recovery} = producer,
        request_id,
        now,
        max_processing_delay_ms,
        in_flight_ts
      )
      when not is_nil(request_id) do
    emit(producer, :completed, %{duration_ms: recovery.monotonic_fun.() - recovery.last_issued_at}, %{
      request_id: request_id
    })

    producer = %{producer | recovery: %{recovery | job: nil}, last_alive_at: now}
    freshness = content_freshness(producer, in_flight_ts)

    if processing_violation?(freshness, now, max_processing_delay_ms),
      do: {:ok, mark_delayed(producer, now - freshness)},
      else: {:ok, mark_up(producer)}
  end

  def complete_recovery(%__MODULE__{}, _request_id, _now, _max_processing_delay_ms, _in_flight_ts), do: :stale

  @doc false
  @spec park_recovery(t()) :: t()
  def park_recovery(%__MODULE__{} = producer) do
    if recovering?(producer) and not recovery_pending?(producer), do: mark_recovery_pending(producer), else: producer
  end

  @doc false
  @spec recovering?(t()) :: boolean()
  def recovering?(%__MODULE__{recovery: %{job: job}}) when not is_nil(job), do: true
  def recovering?(%__MODULE__{}), do: false

  @doc false
  @spec awaiting_alive?(t()) :: boolean()
  def awaiting_alive?(%__MODULE__{recovery: %{job: %{awaiting_alive: true}}}), do: true
  def awaiting_alive?(%__MODULE__{}), do: false

  @doc false
  @spec recovery_pending?(t()) :: boolean()
  def recovery_pending?(%__MODULE__{recovery: %{job: %{request_id: nil}}}), do: true
  def recovery_pending?(%__MODULE__{}), do: false

  @doc false
  @spec initiate_recovery(t()) :: t()
  def initiate_recovery(%__MODULE__{recovery: recovery} = producer) do
    remaining =
      case recovery.last_issued_at do
        nil -> 0
        last -> max(recovery.min_interval_ms - (recovery.monotonic_fun.() - last), 0)
      end

    case remaining do
      0 -> issue_recovery(producer)
      milliseconds -> defer_recovery(producer, milliseconds)
    end
  end

  @doc false
  @spec handle_retry(t(), reference()) :: t()
  def handle_retry(%__MODULE__{recovery: %{job: %{request_id: nil, generation: generation}}} = producer, generation),
    do: issue_recovery(producer)

  def handle_retry(%__MODULE__{} = producer, _generation), do: producer

  @doc false
  @spec handle_stall(t(), integer()) :: t()
  def handle_stall(%__MODULE__{recovery: %{job: %{request_id: request_id}}} = producer, request_id)
      when not is_nil(request_id) do
    Logger.warning("UOF.SDK.ProducerMonitor: producer #{producer.id} recovery #{request_id} stalled; reissuing")
    emit(producer, :stalled, %{}, %{request_id: request_id})
    producer |> mark_recovery_pending() |> issue_recovery()
  end

  def handle_stall(%__MODULE__{} = producer, _request_id), do: producer

  defp defer_recovery(producer, remaining) do
    Logger.info("UOF.SDK.ProducerMonitor: producer #{producer.id} recovery deferred for #{remaining}ms")
    schedule({:retry, producer.id, producer.recovery.job.generation}, remaining)
    producer
  end

  defp issue_recovery(%__MODULE__{recovery: %{job: job} = recovery} = producer) do
    after_ts = job.after_ts
    request_id = recovery.next_request_id
    opts = build_opts(after_ts, request_id, recovery.node_id)
    job = %{job | request_id: request_id}

    producer = %{
      producer
      | recovery: %{
          recovery
          | job: job,
            next_request_id: next_request_id(request_id),
            last_issued_at: recovery.monotonic_fun.()
        }
    }

    # The request runs in a task linked to the monitor, so a slow API cannot
    # block it, while an exception still crashes it as before. The response
    # returns as a message for handle_response/2.
    product = producer.product
    task = Task.async(fn -> send_recovery_request(recovery.request, product, opts) end)
    put_in(producer.recovery.job.task, task.ref)
  end

  @doc false
  @spec awaiting_response?(t(), reference()) :: boolean()
  def awaiting_response?(%__MODULE__{recovery: %{job: %{task: ref}}}, ref) when is_reference(ref), do: true
  def awaiting_response?(%__MODULE__{}, _ref), do: false

  # Only called for the job's current task: a response for a completed,
  # parked, or replaced job no longer matches and is dropped by the monitor.
  @doc false
  @spec handle_response(t(), :ok | {:error, term()}) :: t()
  def handle_response(%__MODULE__{recovery: %{job: job} = recovery} = producer, result) do
    producer = put_in(producer.recovery.job.task, nil)

    case result do
      :ok ->
        emit_initiated(producer, job.request_id, job.after_ts)
        schedule({:stall, producer.id, job.request_id}, recovery.max_recovery_ms)
        producer

      {:error, reason} ->
        emit(producer, :failed, %{}, %{request_id: job.request_id, reason: reason, retry_in_ms: recovery.min_interval_ms})

        Logger.warning(
          "UOF.SDK.ProducerMonitor: producer #{producer.id} recovery request failed; " <>
            "retrying in #{recovery.min_interval_ms}ms"
        )

        producer = mark_recovery_pending(producer)
        schedule({:retry, producer.id, producer.recovery.job.generation}, recovery.min_interval_ms)
        producer
    end
  end

  # A 2xx acknowledgement can still carry a non-ACCEPTED response code; no
  # `snapshot_complete` follows a rejection, so it must stay pending.
  # Exceptions are not rescued: recovery intent is already durable, so a crash
  # restarts with recovery still required.
  defp send_recovery_request(recover_fun, product, opts) do
    case recover_fun.(product, opts) do
      {:ok, %Response{response_code: code} = response} when code != "ACCEPTED" ->
        log_failure("#{code}: #{response.message || response.errors || "(no message)"}")

      {:ok, _response} ->
        :ok

      {:error, %UOF.API.Error{} = error} ->
        log_failure(Exception.message(error))

      {:error, reason} ->
        log_failure(inspect(reason))
    end
  end

  defp log_failure(detail) do
    Logger.warning("UOF.SDK.ProducerMonitor: recover request failed: #{detail}")
    {:error, detail}
  end

  # `snapshot_complete` correlation trusts request ids, so sequences from
  # different VM boots and Pulsar control-plane owners must not overlap: a
  # completion left in a retained backlog, or one for the previous owner's
  # request, would otherwise complete a newer request before its replay
  # arrives. Each producer's sequence starts at a random offset, as the
  # official SDKs do, and wraps within the Java SDK's range. Completion is
  # correlated per producer, so producers need not share a sequence.
  defp next_request_id(@max_request_id), do: 1
  defp next_request_id(request_id), do: request_id + 1

  defp build_opts(after_ts, request_id, node_id) do
    [request_id: request_id]
    |> maybe_put(:node_id, node_id)
    |> maybe_put(:after, after_ts)
  end

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp emit_initiated(producer, request_id, after_ts) do
    emit(producer, :initiated, %{}, %{request_id: request_id, recovery_from: after_ts})

    Logger.info(
      "UOF.SDK.ProducerMonitor: producer #{producer.id} (#{producer.product}) recovery initiated " <>
        "request_id=#{request_id} after=#{inspect(after_ts)}"
    )
  end

  defp emit(producer, event, measurements, metadata) do
    :telemetry.execute(
      [:uof_sdk, :recovery, event],
      Map.put(measurements, :system_time, System.system_time()),
      Map.merge(%{producer_id: producer.id, product: producer.product}, metadata)
    )
  end

  # Producer functions execute inside the ProducerMonitor process, so these
  # messages return to that GenServer without introducing a process per
  # producer.
  defp schedule(message, timeout), do: Process.send_after(self(), message, timeout)

  defp mark_delayed(%__MODULE__{} = producer, delay) do
    %{producer | status: :delayed, processing_queue_delay: delay}
  end

  defp mark_up(%__MODULE__{} = producer) do
    %{producer | status: :up, processing_queue_delay: nil}
  end

  # A resuming producer that has not heard a heartbeat since this instance took
  # control gets one interval, measured from `resuming_since`.
  defp alive_violation?(%__MODULE__{last_alive_at: nil, status: :resuming, resuming_since: since}, now, ms)
       when is_integer(since), do: now - since > ms

  defp alive_violation?(%__MODULE__{last_alive_at: nil}, _now, _ms), do: false
  defp alive_violation?(%__MODULE__{last_alive_at: timestamp}, now, ms), do: now - timestamp > ms

  # Content timestamps include event messages and content-session alives. A
  # fresh system alive is intentionally not evidence that content caught up.
  # The oldest message still being handled bounds how far processing lags;
  # without one, the newest completed message does.
  defp content_freshness(_producer, in_flight_ts) when is_integer(in_flight_ts), do: in_flight_ts
  defp content_freshness(%__MODULE__{last_message_timestamp: timestamp}, nil), do: timestamp

  defp processing_violation?(nil, _now, _ms), do: false
  defp processing_violation?(freshness, now, ms), do: now - freshness > ms

  defp max_timestamp(nil, timestamp), do: timestamp
  defp max_timestamp(timestamp, nil), do: timestamp
  defp max_timestamp(a, b), do: max(a, b)

  defp monotonic_ms, do: System.monotonic_time(:millisecond)
end

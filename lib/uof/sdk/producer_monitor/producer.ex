defmodule UOF.SDK.ProducerMonitor.Producer do
  @moduledoc """
  State machine for one Betradar producer.

  The struct keeps the producer description, health observations, and its
  complete recovery state together: static request configuration, cooldown
  history, and an optional pending or in-flight job. `UOF.SDK.ProducerMonitor`
  supplies global inputs such as session readiness and control-plane
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
  choose different reissue times. The job retains its original timestamp; each
  HTTP attempt starts 60 seconds before it and is bounded by the producer's
  current recovery window with a 10-minute safety margin.

  ## Interrupted recoveries

  An unsubscribed alive or a heartbeat gap while a request is in flight
  interrupts the job: the producer may have lost messages that the running
  replay does not cover. The job keeps waiting for its `snapshot_complete`,
  which then repeats the recovery from the last subscribed alive seen before
  the interruption instead of marking the producer `:up`, as the official SDKs
  do. A job reissued from its original timestamp (stall, failure, demotion,
  session change) covers any interruption, so reissuing clears it.
  """

  alias UOF.Schemas.Common.Response

  require Logger

  @default_min_interval_ms 30_000
  @default_max_recovery_ms 10 * 60_000
  @max_request_id 1_000_000
  @recovery_overlap_ms 60_000
  @recovery_window_margin_ms 10 * 60_000

  @type status :: :down | :up | :resuming

  # `after_ts` is the raw replay origin retained across attempts;
  # `attempt_after_ts` is the value sent to the API for the current request.
  @type recovery_job :: %{
          after_ts: integer() | nil,
          attempt_after_ts: integer() | nil,
          request_id: integer() | nil,
          generation: reference(),
          awaiting_alive: boolean(),
          task: reference() | nil,
          interrupted: boolean(),
          resume_ts: integer() | nil
        }

  @type recovery_state :: %{
          job: recovery_job() | nil,
          last_issued_at: integer() | nil,
          request: (term(), keyword() -> term()),
          next_request_id: pos_integer(),
          monotonic_fun: (-> integer()),
          now_fun: (-> integer()),
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
          alive_delay: integer() | nil,
          resuming_since: integer() | nil,
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
          | {:interrupted, t()}
          | {:recovery_needed, t(), :alive_timeout}

  defstruct [
    :id,
    :product,
    :name,
    :recovery_window_minutes,
    :last_alive_at,
    :alive_delay,
    :resuming_since,
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
      now_fun: Keyword.get(opts, :now_fun, fn -> System.system_time(:millisecond) end),
      node_id: Keyword.get(opts, :node_id),
      min_interval_ms: Keyword.get(opts, :min_interval_ms, @default_min_interval_ms),
      max_recovery_ms: Keyword.get(opts, :max_recovery_ms, @default_max_recovery_ms)
    }

    %{producer | recovery: recovery}
  end

  @doc false
  @spec observe_alive(t(), integer() | nil, boolean(), integer()) :: alive_result()
  def observe_alive(%__MODULE__{} = producer, gen_timestamp, subscribed?, now) do
    observed = %{producer | last_alive_at: now, alive_delay: alive_delay(gen_timestamp, now)}

    cond do
      awaiting_alive?(producer) ->
        {:alive_resumed, observed}

      recovery_in_flight?(producer) ->
        {:ok, observe_alive_in_flight(observed, gen_timestamp, subscribed?)}

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
  @spec check(t(), integer(), non_neg_integer(), boolean()) :: check_result()
  def check(producer, now, inactivity_ms, session_ready?) do
    cond do
      recovery_in_flight?(producer) and not producer.recovery.job.interrupted and
          alive_violation?(producer, now, inactivity_ms) ->
        {:interrupted, interrupt(producer, :alive_timeout)}

      recovering?(producer) ->
        :unchanged

      alive_violation?(producer, now, inactivity_ms) ->
        {:recovery_needed, producer, :alive_timeout}

      producer.status == :resuming and current_alive?(producer, inactivity_ms) and session_ready? ->
        {:transition, mark_up(producer)}

      true ->
        :unchanged
    end
  end

  @doc false
  @spec prepare_recovery(t(), integer() | nil, boolean()) :: t()
  def prepare_recovery(%__MODULE__{recovery: recovery} = producer, after_ts, awaiting_alive \\ false) do
    job = %{
      after_ts: after_ts,
      attempt_after_ts: nil,
      request_id: nil,
      generation: make_ref(),
      awaiting_alive: awaiting_alive,
      task: nil,
      interrupted: false,
      resume_ts: nil
    }

    %{producer | recovery: %{recovery | job: job}}
  end

  # The checkpoint is frozen while recovering. The recovery window moves, so
  # the effective replay timestamp is calculated at each HTTP attempt.
  @doc false
  @spec release_recovery(t(), integer() | nil) :: t()
  def release_recovery(%__MODULE__{recovery: %{job: job} = recovery} = producer, after_ts) do
    %{producer | recovery: %{recovery | job: %{job | after_ts: after_ts, awaiting_alive: false}}}
  end

  # A session change is already durable when this is called, so the
  # replacement may proceed directly through cooldown and HTTP issuance.
  @doc false
  @spec restart_recovery(t(), integer() | nil) :: t()
  def restart_recovery(%__MODULE__{} = producer, after_ts) do
    producer |> repeat_recovery(after_ts) |> initiate_recovery()
  end

  # Replaces the job with a pending one replaying from `after_ts`. The caller
  # initiates it, subject to its own gates.
  @doc false
  @spec repeat_recovery(t(), integer() | nil) :: t()
  def repeat_recovery(%__MODULE__{recovery: recovery} = producer, after_ts) do
    job = %{recovery.job | after_ts: after_ts}
    mark_recovery_pending(%{producer | recovery: %{recovery | job: job}})
  end

  # This transition intentionally performs no I/O: stalls reissue immediately,
  # request failures wait for the retry interval, and demotion waits for
  # promotion.
  @spec mark_recovery_pending(t()) :: t()
  defp mark_recovery_pending(%__MODULE__{recovery: recovery} = producer) do
    job = %{
      recovery.job
      | attempt_after_ts: nil,
        request_id: nil,
        generation: make_ref(),
        task: nil,
        interrupted: false,
        resume_ts: nil
    }

    %{producer | recovery: %{recovery | job: job}}
  end

  @doc false
  @spec complete_recovery(t(), integer(), integer()) :: {:ok, t()} | {:repeat, t(), integer() | nil} | :stale
  def complete_recovery(
        %__MODULE__{recovery: %{job: %{request_id: request_id, interrupted: true} = job}} = producer,
        request_id,
        now
      )
      when not is_nil(request_id) do
    resume_ts = job.resume_ts || job.after_ts

    Logger.info(
      "UOF.SDK.ProducerMonitor: producer #{producer.id} recovery #{request_id} completed after an interruption; " <>
        "repeating from #{inspect(resume_ts)}"
    )

    {:repeat, %{producer | last_alive_at: now}, resume_ts}
  end

  def complete_recovery(%__MODULE__{recovery: %{job: %{request_id: request_id}} = recovery} = producer, request_id, now)
      when not is_nil(request_id) do
    emit(producer, :completed, %{duration_ms: recovery.monotonic_fun.() - recovery.last_issued_at}, %{
      request_id: request_id
    })

    {:ok, mark_up(%{producer | recovery: %{recovery | job: nil}, last_alive_at: now})}
  end

  def complete_recovery(%__MODULE__{}, _request_id, _now), do: :stale

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
  @spec recovery_in_flight?(t()) :: boolean()
  def recovery_in_flight?(%__MODULE__{recovery: %{job: %{request_id: request_id}}}) when not is_nil(request_id), do: true
  def recovery_in_flight?(%__MODULE__{}), do: false

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
    Logger.warning(
      "UOF.SDK.ProducerMonitor: producer #{producer.id} recovery #{request_id} stalled: " <>
        "no snapshot_complete within #{producer.recovery.max_recovery_ms}ms; reissuing"
    )

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
    after_ts = recovery_after(job.after_ts, producer.recovery_window_minutes, recovery.now_fun.())
    request_id = recovery.next_request_id
    opts = build_opts(after_ts, request_id, recovery.node_id)
    job = %{job | attempt_after_ts: after_ts, request_id: request_id}

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
  #
  # A superseded request is deliberately left to finish rather than shut down:
  # by then it has usually reached Betradar, so stopping the task would not
  # retract it, and a duplicate request is harmless beyond quota. The task
  # stays linked, so an exception from it still crashes the monitor; the
  # request function returns errors as values, so an exception is a bug.
  @doc false
  @spec handle_response(t(), :ok | {:error, term()}) :: t()
  def handle_response(%__MODULE__{recovery: %{job: job} = recovery} = producer, result) do
    producer = put_in(producer.recovery.job.task, nil)

    case result do
      :ok ->
        emit_initiated(producer, job.request_id, job.attempt_after_ts)
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

  defp recovery_after(nil, _window_minutes, _now), do: nil

  defp recovery_after(timestamp, window_minutes, now) do
    replay_from = max(timestamp - @recovery_overlap_ms, 0)

    case window_minutes do
      nil ->
        replay_from

      minutes ->
        window_ms = minutes * 60_000
        margin_ms = min(@recovery_window_margin_ms, div(window_ms, 2))
        max(replay_from, now - window_ms + margin_ms)
    end
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

  defp mark_up(%__MODULE__{} = producer), do: %{producer | status: :up}

  # Subscribed alives mark how far the feed is known to be continuous while the
  # request runs. After an interruption they no longer prove that.
  defp observe_alive_in_flight(%__MODULE__{recovery: %{job: %{interrupted: true}}} = producer, _gen_ts, _subscribed?),
    do: producer

  defp observe_alive_in_flight(%__MODULE__{} = producer, _gen_ts, false), do: interrupt(producer, :unsubscribed)

  defp observe_alive_in_flight(%__MODULE__{} = producer, gen_ts, true) when is_integer(gen_ts),
    do: put_in(producer.recovery.job.resume_ts, gen_ts)

  defp observe_alive_in_flight(%__MODULE__{} = producer, nil, true), do: producer

  defp interrupt(%__MODULE__{recovery: %{job: job}} = producer, reason) do
    Logger.warning(
      "UOF.SDK.ProducerMonitor: producer #{producer.id} recovery #{job.request_id} interrupted (#{reason}); " <>
        "repeating it once it completes"
    )

    emit(producer, :interrupted, %{}, %{request_id: job.request_id, reason: reason})
    put_in(producer.recovery.job.interrupted, true)
  end

  # A resuming producer that has not heard a heartbeat since this instance took
  # control gets one interval, measured from `resuming_since`.
  # How late the system pipeline read the alive: local receipt time minus its
  # generation timestamp, so it includes clock skew with Betradar.
  defp alive_delay(gen_timestamp, now) when is_integer(gen_timestamp), do: now - gen_timestamp
  defp alive_delay(nil, _now), do: nil

  # Alives drained from retained backlog prove the feed is flowing but not that
  # the backlog has been read: a session change or unsubscribed alive may still
  # be queued behind them. Resuming waits for one generated within the
  # heartbeat interval.
  defp current_alive?(%__MODULE__{last_alive_at: nil}, _ms), do: false
  defp current_alive?(%__MODULE__{alive_delay: delay}, ms), do: is_integer(delay) and delay <= ms

  defp alive_violation?(%__MODULE__{last_alive_at: nil, status: :resuming, resuming_since: since}, now, ms)
       when is_integer(since), do: now - since > ms

  defp alive_violation?(%__MODULE__{last_alive_at: nil}, _now, _ms), do: false
  defp alive_violation?(%__MODULE__{last_alive_at: timestamp}, now, ms), do: now - timestamp > ms

  defp monotonic_ms, do: System.monotonic_time(:millisecond)
end

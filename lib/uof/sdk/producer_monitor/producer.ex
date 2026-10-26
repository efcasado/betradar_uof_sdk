defmodule UOF.SDK.ProducerMonitor.Producer do
  @moduledoc """
  State machine for one Betradar producer: health observations, the recovery
  it owes, the request in flight, and the cooldown. Its functions run in the
  monitor process; each HTTP request runs in a task linked to the monitor,
  whose response returns to it as a message.

  `checkpoint` and `pending_recovery` are the producer's durable progress,
  which the monitor persists after every event (`progress/1`, `restore/3`).
  Everything else is runtime state. Events record facts (`observe_alive/4`,
  `check/3`, `owe/2`, `drop_request/1`, `complete_recovery/3`), and its
  reported status is derived from them (`status/2`). `advance/3` is the only
  function that issues requests: once a recovery is owed, nothing is in
  flight, the producer is heard, and the cooldown since the last attempt has
  passed.

  An unsubscribed alive or a heartbeat gap while a request is in flight
  interrupts it: its completion advances the checkpoint to the last subscribed
  alive seen before, and the recovery is repeated from there instead of
  marking the producer `:up`. Each attempt replays from 60 seconds before the
  checkpoint, bounded by the current recovery window with a 10-minute margin.
  """

  alias UOF.Schemas.Common.Response
  alias UOF.SDK.ProducerMonitor.Snapshot.Progress

  require Logger

  @default_min_interval_ms 30_000
  @default_max_recovery_ms 10 * 60_000
  @max_request_id 1_000_000
  @recovery_overlap_ms 60_000
  @recovery_window_margin_ms 10 * 60_000

  # `after_ts` is the value sent to the API. `resume_ts` is the last subscribed
  # alive seen while the request was in flight and not yet interrupted.
  @type request :: %{
          id: integer(),
          after_ts: integer() | nil,
          task: reference() | nil,
          interrupted: boolean(),
          resume_ts: integer() | nil
        }

  @type recovery_state :: %{
          request: request() | nil,
          last_issued_at: integer() | nil,
          recover_fun: (term(), keyword() -> term()),
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
          checkpoint: integer() | nil,
          pending_recovery: Progress.pending_recovery(),
          last_alive_at: integer() | nil,
          alive_delay: integer() | nil,
          resuming_since: integer() | nil,
          recovery: recovery_state() | nil
        }

  # A producer with no saved progress owes a recovery.
  defstruct [
    :id,
    :product,
    :name,
    :recovery_window_minutes,
    :checkpoint,
    :last_alive_at,
    :alive_delay,
    :resuming_since,
    :recovery,
    pending_recovery: :incremental
  ]

  @doc false
  @spec configure_recovery(t(), keyword()) :: t()
  def configure_recovery(%__MODULE__{} = producer, opts) do
    recovery = %{
      request: nil,
      last_issued_at: nil,
      recover_fun: Keyword.get(opts, :recover_fun, &UOF.API.Recovery.recover/2),
      next_request_id: Keyword.get_lazy(opts, :request_id_start, fn -> :rand.uniform(@max_request_id) end),
      monotonic_fun: Keyword.get(opts, :monotonic_fun, &monotonic_ms/0),
      now_fun: Keyword.get(opts, :now_fun, fn -> System.system_time(:millisecond) end),
      node_id: Keyword.get(opts, :node_id),
      min_interval_ms: Keyword.get(opts, :min_interval_ms, @default_min_interval_ms),
      max_recovery_ms: Keyword.get(opts, :max_recovery_ms, @default_max_recovery_ms)
    }

    %{producer | recovery: recovery}
  end

  @doc """
  Restore durable progress, discarding runtime state, as at startup.

  A producer that owes no recovery and has a checkpoint resumes; any other
  owes a recovery. `resuming_since` starts the heartbeat interval a resuming
  producer gets, without counting as a heartbeat. A request in flight is
  abandoned; its response and completion no longer match.
  """
  @spec restore(t(), Progress.t(), integer()) :: t()
  def restore(%__MODULE__{} = producer, %Progress{} = progress, now) do
    producer = %{
      producer
      | checkpoint: progress.checkpoint,
        last_alive_at: nil,
        alive_delay: nil,
        resuming_since: nil,
        recovery: %{producer.recovery | request: nil}
    }

    case progress do
      %Progress{pending_recovery: nil, checkpoint: checkpoint} when is_integer(checkpoint) ->
        %{producer | pending_recovery: nil, resuming_since: now}

      %Progress{pending_recovery: pending_recovery} ->
        %{producer | pending_recovery: pending_recovery || :incremental}
    end
  end

  @doc "The producer's durable progress."
  @spec progress(t()) :: Progress.t()
  def progress(%__MODULE__{} = producer) do
    %Progress{checkpoint: producer.checkpoint, pending_recovery: producer.pending_recovery}
  end

  @doc false
  @spec observe_alive(t(), integer() | nil, boolean(), integer()) :: t()
  def observe_alive(%__MODULE__{} = producer, gen_timestamp, subscribed?, now) do
    observed = %{producer | last_alive_at: now, alive_delay: alive_delay(gen_timestamp, now)}

    cond do
      recovery_in_flight?(producer) -> observe_alive_in_flight(observed, gen_timestamp, subscribed?)
      not subscribed? -> owe(observed, :incremental)
      up?(producer) -> advance_checkpoint(observed, gen_timestamp)
      true -> observed
    end
  end

  @doc false
  @spec check(t(), integer(), non_neg_integer()) :: t()
  def check(%__MODULE__{} = producer, now, inactivity_ms) do
    cond do
      recovery_in_flight?(producer) and not producer.recovery.request.interrupted and
          silent?(producer, now, inactivity_ms) ->
        interrupt(producer, :alive_timeout)

      recovering?(producer) ->
        producer

      silent?(producer, now, inactivity_ms) ->
        owe(producer, :incremental)

      is_integer(producer.resuming_since) and current_alive?(producer, inactivity_ms) ->
        %{producer | resuming_since: nil}

      true ->
        producer
    end
  end

  @doc """
  Record that the producer owes a recovery of `kind`. A full recovery owed
  stays owed until it completes.
  """
  @spec owe(t(), :incremental | :full) :: t()
  def owe(%__MODULE__{pending_recovery: :full} = producer, _kind), do: producer
  def owe(%__MODULE__{} = producer, kind), do: %{producer | pending_recovery: kind, resuming_since: nil}

  @doc """
  Abandon the request in flight, if any. The recovery stays owed, and
  `advance/3` issues it again.
  """
  @spec drop_request(t()) :: t()
  def drop_request(%__MODULE__{} = producer), do: put_in(producer.recovery.request, nil)

  # The replay from the checkpoint is complete. After an interruption, the
  # subscribed alives seen before it showed the feed continuous up to
  # `resume_ts`, so the checkpoint advances there and the recovery stays owed.
  @doc false
  @spec complete_recovery(t(), integer(), integer()) :: t()
  def complete_recovery(
        %__MODULE__{recovery: %{request: %{id: request_id, interrupted: true} = request}} = producer,
        request_id,
        now
      ) do
    Logger.info(
      "UOF.SDK.ProducerMonitor: producer #{producer.id} recovery #{request_id} completed after an interruption; " <>
        "repeating it"
    )

    %{producer | last_alive_at: now} |> advance_checkpoint(request.resume_ts) |> drop_request()
  end

  def complete_recovery(%__MODULE__{recovery: %{request: %{id: request_id}} = recovery} = producer, request_id, now) do
    emit(producer, :completed, %{duration_ms: recovery.monotonic_fun.() - recovery.last_issued_at}, %{
      request_id: request_id
    })

    Logger.info("UOF.SDK.ProducerMonitor: producer #{producer.id} recovery #{request_id} complete")
    drop_request(%{producer | pending_recovery: nil, last_alive_at: now})
  end

  def complete_recovery(%__MODULE__{} = producer, _request_id, _now), do: producer

  @doc false
  @spec recovering?(t()) :: boolean()
  def recovering?(%__MODULE__{pending_recovery: pending_recovery}), do: not is_nil(pending_recovery)

  @doc """
  The lifecycle status reported to applications. A producer that owes a
  recovery is `:recovering` while its request is in flight or can be issued,
  and `:down` while it is not heard: Betradar accepts recovery only for a
  producer that is alive. `silent?` is the monitor's judgement of
  `silent?/3`.
  """
  @spec status(t(), boolean()) :: UOF.SDK.ProducerStatus.status()
  def status(%__MODULE__{pending_recovery: nil, resuming_since: since}, _silent?) when is_integer(since), do: :resuming
  def status(%__MODULE__{pending_recovery: nil}, _silent?), do: :up

  def status(%__MODULE__{} = producer, silent?) do
    if recovery_in_flight?(producer) or (is_integer(producer.last_alive_at) and not silent?),
      do: :recovering,
      else: :down
  end

  @doc """
  Whether the producer has gone quiet: no alive within `inactivity_ms`, or, for
  a resuming producer not heard since it was restored, none within
  `inactivity_ms` of resuming.
  """
  @spec silent?(t(), integer(), non_neg_integer()) :: boolean()
  def silent?(%__MODULE__{last_alive_at: nil, resuming_since: since}, now, ms) when is_integer(since),
    do: now - since > ms

  def silent?(%__MODULE__{last_alive_at: nil}, _now, _ms), do: false
  def silent?(%__MODULE__{last_alive_at: timestamp}, now, ms), do: now - timestamp > ms

  @doc """
  Issue the producer's recovery request if it is due.

  Called by the monitor only while it may issue. An accepted request without
  `snapshot_complete` for longer than `max_recovery_ms` is dropped first, so
  it is reissued like any owed recovery. A recovery is due once no request is
  in flight, the producer is heard, and the cooldown since the last attempt
  has passed.
  """
  @spec advance(t(), integer(), non_neg_integer()) :: t()
  def advance(%__MODULE__{} = producer, now, inactivity_ms) do
    producer = drop_stalled_request(producer)

    if due?(producer, now, inactivity_ms), do: issue_recovery(producer), else: producer
  end

  # A request whose HTTP response has not arrived yet is left alone.
  defp drop_stalled_request(%__MODULE__{recovery: %{request: %{task: nil} = request} = recovery} = producer) do
    if elapsed_since_issue(recovery) > recovery.max_recovery_ms do
      Logger.warning(
        "UOF.SDK.ProducerMonitor: producer #{producer.id} recovery #{request.id} stalled: " <>
          "no snapshot_complete within #{recovery.max_recovery_ms}ms; reissuing"
      )

      emit(producer, :stalled, %{}, %{request_id: request.id})
      drop_request(producer)
    else
      producer
    end
  end

  defp drop_stalled_request(%__MODULE__{} = producer), do: producer

  defp due?(%__MODULE__{pending_recovery: nil}, _now, _inactivity_ms), do: false

  defp due?(%__MODULE__{recovery: recovery} = producer, now, inactivity_ms) do
    is_nil(recovery.request) and heard?(producer, now, inactivity_ms) and
      elapsed_since_issue(recovery) >= recovery.min_interval_ms
  end

  # Betradar accepts recovery only for a producer that is alive.
  defp heard?(producer, now, inactivity_ms) do
    is_integer(producer.last_alive_at) and not silent?(producer, now, inactivity_ms)
  end

  defp elapsed_since_issue(%{last_issued_at: nil}), do: :infinity
  defp elapsed_since_issue(%{last_issued_at: last} = recovery), do: recovery.monotonic_fun.() - last

  # A full recovery, or one without a checkpoint, omits `after`.
  defp issue_recovery(%__MODULE__{recovery: recovery} = producer) do
    from = if producer.pending_recovery == :full, do: nil, else: producer.checkpoint
    after_ts = recovery_after(from, producer.recovery_window_minutes, recovery.now_fun.())
    request_id = recovery.next_request_id
    opts = build_opts(after_ts, request_id, recovery.node_id)

    # The request runs in a task linked to the monitor, so a slow API cannot
    # block it, while an exception still crashes it as before. The response
    # returns as a message for handle_response/2.
    product = producer.product
    task = Task.async(fn -> send_recovery_request(recovery.recover_fun, product, opts) end)

    request = %{id: request_id, after_ts: after_ts, task: task.ref, interrupted: false, resume_ts: nil}

    %{
      producer
      | recovery: %{
          recovery
          | request: request,
            next_request_id: next_request_id(request_id),
            last_issued_at: recovery.monotonic_fun.()
        }
    }
  end

  @doc false
  @spec awaiting_response?(t(), reference()) :: boolean()
  def awaiting_response?(%__MODULE__{recovery: %{request: %{task: ref}}}, ref) when is_reference(ref), do: true
  def awaiting_response?(%__MODULE__{}, _ref), do: false

  # Only called for the current request's task: a response for a completed,
  # abandoned, or replaced request no longer matches and is dropped by the
  # monitor.
  #
  # A superseded request is deliberately left to finish rather than shut down:
  # by then it has usually reached Betradar, so stopping the task would not
  # retract it, and a duplicate request is harmless beyond quota. The task
  # stays linked, so an exception from it still crashes the monitor; the
  # request function returns errors as values, so an exception is a bug.
  @doc false
  @spec handle_response(t(), :ok | {:error, term()}) :: t()
  def handle_response(%__MODULE__{recovery: %{request: request} = recovery} = producer, result) do
    case result do
      :ok ->
        emit_initiated(producer, request.id, request.after_ts)
        put_in(producer.recovery.request.task, nil)

      # The retry waits a full cooldown from the failure, however long the
      # request took.
      {:error, reason} ->
        emit(producer, :failed, %{}, %{request_id: request.id, reason: reason, retry_in_ms: recovery.min_interval_ms})

        Logger.warning(
          "UOF.SDK.ProducerMonitor: producer #{producer.id} recovery request failed; " <>
            "retrying in #{recovery.min_interval_ms}ms"
        )

        %{producer | recovery: %{recovery | request: nil, last_issued_at: recovery.monotonic_fun.()}}
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

  defp recovery_in_flight?(%__MODULE__{recovery: %{request: request}}), do: not is_nil(request)

  defp up?(%__MODULE__{pending_recovery: nil, resuming_since: nil}), do: true
  defp up?(%__MODULE__{}), do: false

  # Checkpoints only move forward.
  defp advance_checkpoint(%__MODULE__{} = producer, nil), do: producer

  defp advance_checkpoint(%__MODULE__{checkpoint: checkpoint} = producer, timestamp)
       when is_integer(checkpoint) and checkpoint >= timestamp, do: producer

  defp advance_checkpoint(%__MODULE__{} = producer, timestamp), do: %{producer | checkpoint: timestamp}

  # Subscribed alives mark how far the feed is known to be continuous while the
  # request runs. After an interruption they no longer prove that.
  defp observe_alive_in_flight(%__MODULE__{recovery: %{request: %{interrupted: true}}} = producer, _gen_ts, _subscribed?),
    do: producer

  defp observe_alive_in_flight(%__MODULE__{} = producer, _gen_ts, false), do: interrupt(producer, :unsubscribed)

  defp observe_alive_in_flight(%__MODULE__{} = producer, gen_ts, true) when is_integer(gen_ts),
    do: put_in(producer.recovery.request.resume_ts, gen_ts)

  defp observe_alive_in_flight(%__MODULE__{} = producer, nil, true), do: producer

  defp interrupt(%__MODULE__{recovery: %{request: request}} = producer, reason) do
    Logger.warning(
      "UOF.SDK.ProducerMonitor: producer #{producer.id} recovery #{request.id} interrupted (#{reason}); " <>
        "repeating it once it completes"
    )

    emit(producer, :interrupted, %{}, %{request_id: request.id, reason: reason})
    put_in(producer.recovery.request.interrupted, true)
  end

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

  defp monotonic_ms, do: System.monotonic_time(:millisecond)
end

defmodule UOF.SDK.ProducerMonitor.Producer do
  @moduledoc """
  Tracks one producer's health, checkpoint, owed recovery, and active request.

  The monitor persists `checkpoint` and `pending_recovery`; all other fields
  are runtime state. Transitions receive time explicitly and emit diagnostics.
  `UOF.SDK.ProducerMonitor.Requests` executes HTTP requests.

  See `docs/recovery.md` for the recovery lifecycle and heartbeat rules.
  """

  alias UOF.SDK.ProducerMonitor.Snapshot.Progress

  require Logger

  # `after_ts` is the value sent to the API, and `accepted` whether the API
  # has accepted the request. `resume_ts` is the last subscribed alive seen
  # while the request was in flight and not yet interrupted.
  @type request :: %{
          id: integer(),
          after_ts: integer() | nil,
          accepted: boolean(),
          interrupted: boolean(),
          resume_ts: integer() | nil
        }

  @type t :: %__MODULE__{
          id: integer(),
          product: String.t() | nil,
          name: String.t() | nil,
          recovery_window_minutes: integer() | nil,
          checkpoint: integer() | nil,
          pending_recovery: Progress.pending_recovery(),
          last_alive_at: integer() | nil,
          last_generated_at: integer() | nil,
          alive_delay: integer() | nil,
          restored_at: integer() | nil,
          request: request() | nil,
          last_issued_at: integer() | nil
        }

  # A producer with no saved progress owes a recovery.
  defstruct [
    :id,
    :product,
    :name,
    :recovery_window_minutes,
    :checkpoint,
    :last_alive_at,
    :last_generated_at,
    :alive_delay,
    :restored_at,
    :request,
    :last_issued_at,
    pending_recovery: :incremental
  ]

  @doc """
  Restore durable progress, discarding runtime state, as at startup.

  A producer that owes no recovery and has a checkpoint resumes without one;
  any other owes a recovery. Either way it is `:down` until heard, and
  `restored_at` starts the heartbeat interval it gets, without counting as a
  heartbeat. A request in flight is abandoned; its response and completion no
  longer match.
  """
  @spec restore(t(), Progress.t(), integer()) :: t()
  def restore(%__MODULE__{} = producer, %Progress{} = progress, now) do
    pending_recovery =
      if is_nil(progress.pending_recovery) and is_integer(progress.checkpoint),
        do: nil,
        else: progress.pending_recovery || :incremental

    %{
      producer
      | checkpoint: progress.checkpoint,
        pending_recovery: pending_recovery,
        last_alive_at: nil,
        last_generated_at: nil,
        alive_delay: nil,
        restored_at: now,
        request: nil
    }
  end

  @doc "The producer's durable progress."
  @spec progress(t()) :: Progress.t()
  def progress(%__MODULE__{} = producer) do
    %Progress{checkpoint: producer.checkpoint, pending_recovery: producer.pending_recovery}
  end

  @doc false
  @spec observe_alive(t(), integer() | nil, boolean(), integer(), non_neg_integer()) :: t()
  def observe_alive(%__MODULE__{} = producer, gen_timestamp, subscribed?, now, inactivity_ms) do
    observed = %{
      producer
      | last_alive_at: now,
        last_generated_at: latest(producer.last_generated_at, gen_timestamp),
        alive_delay: alive_delay(gen_timestamp, now)
    }

    # The first alive after a restore is compared with the checkpoint: the last
    # subscribed alive the stored progress accounts for.
    previous = producer.last_generated_at || producer.checkpoint

    cond do
      in_flight?(producer) -> observe_alive_in_flight(observed, gen_timestamp, subscribed?)
      not subscribed? -> owe(observed, :incremental)
      owes_recovery?(producer) -> observed
      gap?(previous, gen_timestamp, inactivity_ms) -> owe_after_gap(observed, previous, gen_timestamp)
      true -> advance_checkpoint(observed, gen_timestamp)
    end
  end

  @doc false
  @spec check(t(), integer(), non_neg_integer()) :: t()
  def check(%__MODULE__{} = producer, now, inactivity_ms) do
    cond do
      in_flight?(producer) and not producer.request.interrupted and silent?(producer, now, inactivity_ms) ->
        interrupt(producer, :alive_timeout)

      owes_recovery?(producer) ->
        producer

      silent?(producer, now, inactivity_ms) ->
        owe(producer, :incremental)

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

  # An incremental request in flight cannot satisfy a full recovery: its
  # completion would clear it. Abandoning it reissues the recovery as full.
  def owe(%__MODULE__{pending_recovery: :incremental} = producer, :full),
    do: %{drop_request(producer) | pending_recovery: :full}

  def owe(%__MODULE__{} = producer, kind), do: %{producer | pending_recovery: kind}

  @doc """
  Abandon the request in flight, if any. The recovery stays owed, and is
  issued again once it is due.
  """
  @spec drop_request(t()) :: t()
  def drop_request(%__MODULE__{} = producer), do: %{producer | request: nil}

  # The replay from the checkpoint is complete, and the subscribed alives seen
  # while it ran showed the feed continuous up to `resume_ts`, so the
  # checkpoint advances there. After an interruption, the recovery stays owed
  # and repeats from it.
  @doc false
  @spec complete_recovery(t(), integer(), integer(), integer()) :: t()
  def complete_recovery(%__MODULE__{request: %{id: request_id} = request} = producer, request_id, now, monotonic_now) do
    producer = %{producer | last_alive_at: now} |> advance_checkpoint(request.resume_ts) |> drop_request()

    if request.interrupted do
      Logger.info(
        "UOF.SDK.ProducerMonitor: producer #{producer.id} recovery #{request_id} completed after an interruption; " <>
          "repeating it"
      )

      producer
    else
      emit(producer, :completed, %{duration_ms: monotonic_now - producer.last_issued_at}, %{request_id: request_id})
      Logger.info("UOF.SDK.ProducerMonitor: producer #{producer.id} recovery #{request_id} complete")
      %{producer | pending_recovery: nil}
    end
  end

  def complete_recovery(%__MODULE__{} = producer, _request_id, _now, _monotonic_now), do: producer

  @doc """
  Whether the producer owes a recovery that covers one of `kind`: any owed
  recovery covers an incremental one, and only a full one a full one. Owing a
  recovery is not the same as the `:recovering` status: a producer that is
  not heard owes one but is `:down`.
  """
  @spec owes_recovery?(t(), :incremental | :full) :: boolean()
  def owes_recovery?(producer, kind \\ :incremental)
  def owes_recovery?(%__MODULE__{pending_recovery: owed}, :incremental), do: not is_nil(owed)
  def owes_recovery?(%__MODULE__{pending_recovery: owed}, :full), do: owed == :full

  @doc """
  The lifecycle status reported to applications: `:down` while the producer
  is not heard, otherwise `:recovering` while it owes a recovery, otherwise
  `:up`. `heard?` is the monitor's judgement of `heard?/3`.
  """
  @spec status(t(), boolean()) :: UOF.SDK.ProducerStatus.status()
  def status(%__MODULE__{}, false), do: :down
  def status(%__MODULE__{pending_recovery: nil}, true), do: :up
  def status(%__MODULE__{}, true), do: :recovering

  @doc """
  Whether the producer has been heard within `inactivity_ms`. Betradar accepts
  recovery only for a producer that is alive.
  """
  @spec heard?(t(), integer(), non_neg_integer()) :: boolean()
  def heard?(%__MODULE__{} = producer, now, inactivity_ms) do
    is_integer(producer.last_alive_at) and not silent?(producer, now, inactivity_ms)
  end

  # Whether the producer has gone quiet: no alive within `inactivity_ms`, or
  # none within `inactivity_ms` of being restored. A producer added at runtime
  # is not silent before its first alive.
  defp silent?(%__MODULE__{last_alive_at: nil, restored_at: nil}, _now, _ms), do: false
  defp silent?(%__MODULE__{last_alive_at: nil, restored_at: since}, now, ms), do: now - since > ms
  defp silent?(%__MODULE__{last_alive_at: timestamp}, now, ms), do: now - timestamp > ms

  @doc """
  Whether a request is due: a recovery is owed, nothing is in flight, the
  producer is heard, and `min_interval_ms` has passed since the last attempt.
  """
  @spec due?(t(), integer(), integer(), non_neg_integer(), non_neg_integer()) :: boolean()
  def due?(%__MODULE__{pending_recovery: nil}, _now, _monotonic_now, _inactivity_ms, _min_interval_ms), do: false

  def due?(%__MODULE__{} = producer, now, monotonic_now, inactivity_ms, min_interval_ms) do
    is_nil(producer.request) and heard?(producer, now, inactivity_ms) and
      elapsed_since_issue(producer, monotonic_now) >= min_interval_ms
  end

  @doc """
  The timestamp the next request replays from: the checkpoint, or `nil` for a
  full recovery, or one without a checkpoint.
  """
  @spec replay_from(t()) :: integer() | nil
  def replay_from(%__MODULE__{pending_recovery: :full}), do: nil
  def replay_from(%__MODULE__{checkpoint: checkpoint}), do: checkpoint

  @doc false
  @spec start_request(t(), integer(), integer() | nil, integer()) :: t()
  def start_request(%__MODULE__{} = producer, request_id, after_ts, monotonic_now) do
    request = %{id: request_id, after_ts: after_ts, accepted: false, interrupted: false, resume_ts: nil}
    %{producer | request: request, last_issued_at: monotonic_now}
  end

  @doc """
  Drop an accepted request without `snapshot_complete` for longer than
  `max_recovery_ms`, so it is reissued like any owed recovery. A request whose
  HTTP response has not arrived yet is left alone.
  """
  @spec drop_stalled_request(t(), integer(), pos_integer()) :: t()
  def drop_stalled_request(%__MODULE__{request: %{accepted: true} = request} = producer, monotonic_now, max_recovery_ms) do
    if elapsed_since_issue(producer, monotonic_now) > max_recovery_ms do
      Logger.warning(
        "UOF.SDK.ProducerMonitor: producer #{producer.id} recovery #{request.id} stalled: " <>
          "no snapshot_complete within #{max_recovery_ms}ms; reissuing"
      )

      emit(producer, :stalled, %{}, %{request_id: request.id})
      drop_request(producer)
    else
      producer
    end
  end

  def drop_stalled_request(%__MODULE__{} = producer, _monotonic_now, _max_recovery_ms), do: producer

  # A response for a completed, abandoned, or replaced request no longer
  # matches the request in flight and is dropped. A failed request is retried
  # `retry_in_ms` after the failure, however long it took.
  @doc false
  @spec handle_response(t(), integer(), :ok | {:error, term()}, integer(), non_neg_integer()) :: t()
  def handle_response(%__MODULE__{request: %{id: request_id} = request} = producer, request_id, :ok, _now, _retry) do
    emit(producer, :initiated, %{}, %{request_id: request.id, recovery_from: request.after_ts})

    Logger.info(
      "UOF.SDK.ProducerMonitor: producer #{producer.id} (#{producer.product}) recovery initiated " <>
        "request_id=#{request.id} after=#{inspect(request.after_ts)}"
    )

    put_in(producer.request.accepted, true)
  end

  def handle_response(
        %__MODULE__{request: %{id: request_id} = request} = producer,
        request_id,
        {:error, reason},
        monotonic_now,
        retry_in_ms
      ) do
    emit(producer, :failed, %{}, %{request_id: request.id, reason: reason, retry_in_ms: retry_in_ms})

    Logger.warning(
      "UOF.SDK.ProducerMonitor: producer #{producer.id} recovery request failed; retrying in #{retry_in_ms}ms"
    )

    %{producer | request: nil, last_issued_at: monotonic_now}
  end

  def handle_response(%__MODULE__{} = producer, _request_id, _result, _monotonic_now, _retry_in_ms), do: producer

  defp elapsed_since_issue(%__MODULE__{last_issued_at: nil}, _monotonic_now), do: :infinity
  defp elapsed_since_issue(%__MODULE__{last_issued_at: last}, monotonic_now), do: monotonic_now - last

  defp emit(producer, event, measurements, metadata) do
    :telemetry.execute(
      [:uof_sdk, :recovery, event],
      Map.put(measurements, :system_time, System.system_time()),
      Map.merge(%{producer_id: producer.id, product: producer.product}, metadata)
    )
  end

  defp in_flight?(%__MODULE__{request: request}), do: not is_nil(request)

  # Checkpoints only move forward.
  defp advance_checkpoint(%__MODULE__{} = producer, nil), do: producer

  defp advance_checkpoint(%__MODULE__{checkpoint: checkpoint} = producer, timestamp)
       when is_integer(checkpoint) and checkpoint >= timestamp, do: producer

  defp advance_checkpoint(%__MODULE__{} = producer, timestamp), do: %{producer | checkpoint: timestamp}

  # Subscribed alives mark how far the feed is known to be continuous while the
  # request runs. After an interruption they no longer prove that.
  defp observe_alive_in_flight(%__MODULE__{request: %{interrupted: true}} = producer, _gen_ts, _subscribed?), do: producer

  defp observe_alive_in_flight(%__MODULE__{} = producer, _gen_ts, false), do: interrupt(producer, :unsubscribed)

  defp observe_alive_in_flight(%__MODULE__{} = producer, gen_ts, true) when is_integer(gen_ts),
    do: put_in(producer.request.resume_ts, gen_ts)

  defp observe_alive_in_flight(%__MODULE__{} = producer, nil, true), do: producer

  defp interrupt(%__MODULE__{request: request} = producer, reason) do
    Logger.warning(
      "UOF.SDK.ProducerMonitor: producer #{producer.id} recovery #{request.id} interrupted (#{reason}); " <>
        "repeating it once it completes"
    )

    emit(producer, :interrupted, %{}, %{request_id: request.id, reason: reason})
    put_in(producer.request.interrupted, true)
  end

  # Compare generation times so draining a backlog does not hide a gap.
  defp gap?(previous, gen_ts, inactivity_ms) when is_integer(previous) and is_integer(gen_ts),
    do: gen_ts - previous > inactivity_ms

  defp gap?(_previous, _gen_ts, _inactivity_ms), do: false

  defp owe_after_gap(%__MODULE__{} = producer, previous, gen_ts) do
    Logger.warning(
      "UOF.SDK.ProducerMonitor: producer #{producer.id} alives generated at #{previous} and #{gen_ts} " <>
        "are further apart than the heartbeat interval; recovering"
    )

    owe(producer, :incremental)
  end

  defp latest(nil, timestamp), do: timestamp
  defp latest(previous, nil), do: previous
  defp latest(previous, timestamp), do: max(previous, timestamp)

  # How late the system pipeline read the alive: local receipt time minus its
  # generation timestamp, so it includes clock skew with Betradar.
  defp alive_delay(gen_timestamp, now) when is_integer(gen_timestamp), do: now - gen_timestamp
  defp alive_delay(nil, _now), do: nil
end

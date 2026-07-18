defmodule UOF.SDK.ProducerMonitor.Producer do
  @moduledoc """
  State machine for one Betradar producer.

  The struct keeps the producer description, health observations, and its
  complete recovery state together: static request configuration, cooldown
  history, and an optional pending or in-flight job. `UOF.SDK.ProducerMonitor`
  supplies global inputs such as connection readiness and control-plane
  ownership. This module executes per-producer recovery HTTP calls, timers,
  logging, and telemetry; the monitor retains event routing, durable
  persistence, and callbacks.

  `status` is the public lifecycle state. `:recovering` is projected from the
  canonical recovery job by `public/1`; it is not duplicated in the underlying
  health state.

    * `:down` — not synchronized and no recovery is pending
    * `:recovering` — waiting to request, requesting, or awaiting recovery
      completion
    * `:up` — synchronized and safe
    * `:delayed` — the remote feed is healthy but local processing is behind
    * `:resuming` — draining retained backlog after a restart and awaiting
      current-session confirmation
  """

  alias UOF.Schemas.Common.Response

  require Logger

  @default_min_interval_ms 30_000
  @default_max_recovery_ms 60 * 60_000

  @type status :: :down | :recovering | :up | :delayed | :resuming

  @type recovery_job :: %{
          after_ts: integer() | nil,
          request_id: integer() | nil,
          generation: reference()
        }

  @type recovery_state :: %{
          job: recovery_job() | nil,
          last_issued_at: integer() | nil,
          request: (term(), keyword() -> term()),
          gen_request_id: (-> integer()),
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
          last_message_timestamp: integer() | nil,
          processing_queue_delay: integer() | nil,
          recovery: recovery_state() | nil
        }

  @type alive_result ::
          {:ok, t()}
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
      gen_request_id:
        Keyword.get(opts, :gen_request_id, fn ->
          System.unique_integer([:positive, :monotonic])
        end),
      monotonic_fun: Keyword.get(opts, :monotonic_fun, &monotonic_ms/0),
      node_id: Keyword.get(opts, :node_id),
      min_interval_ms: Keyword.get(opts, :min_interval_ms, @default_min_interval_ms),
      max_recovery_ms: Keyword.get(opts, :max_recovery_ms, @default_max_recovery_ms)
    }

    %{producer | recovery: recovery}
  end

  @doc false
  @spec public(t()) :: t()
  def public(%__MODULE__{} = producer) do
    producer =
      if recovering?(producer) do
        %{producer | status: :recovering, processing_queue_delay: nil}
      else
        producer
      end

    %{producer | recovery: nil}
  end

  @doc false
  @spec observe_alive(t(), integer() | nil, boolean(), integer()) :: alive_result()
  def observe_alive(%__MODULE__{} = producer, gen_timestamp, subscribed?, now) do
    observed = %{producer | last_alive_at: now}

    cond do
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
  @spec check(t(), integer(), non_neg_integer(), non_neg_integer(), boolean()) :: check_result()
  def check(producer, now, inactivity_ms, max_processing_delay_ms, connections_ready?) do
    cond do
      recovering?(producer) ->
        :unchanged

      alive_violation?(producer, now, inactivity_ms) ->
        {:recovery_needed, producer, :alive_timeout}

      producer.status == :up and processing_violation?(producer, now, max_processing_delay_ms) ->
        {:transition, mark_delayed(producer, now - producer.last_message_timestamp)}

      producer.status == :delayed and
          not processing_violation?(producer, now, max_processing_delay_ms) ->
        {:transition, mark_up(producer)}

      producer.status == :resuming and producer.last_alive_at != nil and connections_ready? and
          not processing_violation?(producer, now, max_processing_delay_ms) ->
        {:transition, mark_up(producer)}

      true ->
        :unchanged
    end
  end

  @doc false
  @spec require_recovery(t(), integer() | nil) :: t()
  def require_recovery(%__MODULE__{recovery: recovery} = producer, after_ts) do
    job = %{after_ts: after_ts, request_id: nil, generation: make_ref()}
    %{producer | recovery: %{recovery | job: job}}
  end

  @doc false
  @spec replace_recovery(t(), integer() | nil) :: t()
  def replace_recovery(%__MODULE__{recovery: recovery} = producer, after_ts) do
    job = %{recovery.job | after_ts: after_ts, request_id: nil, generation: make_ref()}
    %{producer | recovery: %{recovery | job: job}}
  end

  @spec retry_recovery(t()) :: t()
  defp retry_recovery(%__MODULE__{recovery: recovery} = producer) do
    job = %{recovery.job | request_id: nil, generation: make_ref()}
    %{producer | recovery: %{recovery | job: job}}
  end

  @doc false
  @spec complete_recovery(t(), integer(), integer()) :: {:ok, t()} | :stale
  def complete_recovery(%__MODULE__{recovery: %{job: %{request_id: request_id}} = recovery} = producer, request_id, now)
      when not is_nil(request_id) do
    {:ok,
     %{
       producer
       | recovery: %{recovery | job: nil},
         status: :up,
         last_alive_at: now,
         processing_queue_delay: nil
     }}
  end

  def complete_recovery(%__MODULE__{}, _request_id, _now), do: :stale

  @doc false
  @spec park_recovery(t()) :: t()
  def park_recovery(%__MODULE__{} = producer) do
    if recovering?(producer) and not recovery_pending?(producer), do: retry_recovery(producer), else: producer
  end

  @doc false
  @spec recovering?(t()) :: boolean()
  def recovering?(%__MODULE__{recovery: %{job: job}}) when not is_nil(job), do: true
  def recovering?(%__MODULE__{}), do: false

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
    producer |> retry_recovery() |> issue_recovery()
  end

  def handle_stall(%__MODULE__{} = producer, _request_id), do: producer

  defp defer_recovery(producer, remaining) do
    Logger.info("UOF.SDK.ProducerMonitor: producer #{producer.id} recovery deferred for #{remaining}ms")
    schedule({:retry, producer.id, producer.recovery.job.generation}, remaining)
    producer
  end

  defp issue_recovery(%__MODULE__{recovery: %{job: job} = recovery} = producer) do
    after_ts = job.after_ts
    request_id = recovery.gen_request_id.()
    opts = build_opts(after_ts, request_id, recovery.node_id)
    job = %{job | request_id: request_id}

    producer = %{
      producer
      | recovery: %{recovery | job: job, last_issued_at: recovery.monotonic_fun.()}
    }

    case safe_recover(recovery.request, producer.product, opts) do
      :ok ->
        emit_initiated(producer, request_id, after_ts)
        schedule({:stall, producer.id, request_id}, recovery.max_recovery_ms)
        producer

      :error ->
        Logger.warning(
          "UOF.SDK.ProducerMonitor: producer #{producer.id} recovery request failed; " <>
            "retrying in #{recovery.min_interval_ms}ms"
        )

        producer = retry_recovery(producer)
        schedule({:retry, producer.id, producer.recovery.job.generation}, recovery.min_interval_ms)
        producer
    end
  end

  # The HTTP layer decodes any parseable body — including rejection envelopes
  # (throttling, 403) — into `{:ok, %Response{}}` without surfacing the status
  # code. A rejected request must stay pending because no `snapshot_complete`
  # will be published for it.
  defp safe_recover(recover_fun, product, opts) do
    case recover_fun.(product, opts) do
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

  defp alive_violation?(%__MODULE__{last_alive_at: nil}, _now, _ms), do: false
  defp alive_violation?(%__MODULE__{last_alive_at: timestamp}, now, ms), do: now - timestamp > ms

  # Content timestamps include event messages and content-session alives. A
  # fresh system alive is intentionally not evidence that content caught up.
  defp processing_violation?(%__MODULE__{last_message_timestamp: nil}, _now, _ms), do: false

  defp processing_violation?(%__MODULE__{last_message_timestamp: timestamp}, now, ms) do
    now - timestamp > ms
  end

  defp max_timestamp(nil, timestamp), do: timestamp
  defp max_timestamp(timestamp, nil), do: timestamp
  defp max_timestamp(a, b), do: max(a, b)

  defp monotonic_ms, do: System.monotonic_time(:millisecond)
end

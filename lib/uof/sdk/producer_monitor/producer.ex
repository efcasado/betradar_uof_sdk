defmodule UOF.SDK.ProducerMonitor.Producer do
  @moduledoc """
  State machine for one Betradar producer.

  The struct keeps the producer description, health observations, and its
  canonical recovery job together. `UOF.SDK.ProducerMonitor` supplies global
  inputs such as connection readiness and control-plane ownership. This module
  executes per-producer recovery HTTP calls, timers, logging, and telemetry;
  the monitor retains event routing, durable persistence, and callbacks.

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
  alias UOF.SDK.ProducerMonitor.Recovery

  require Logger

  @default_min_interval_ms 30_000
  @default_max_recovery_ms 60 * 60_000

  @type status :: :down | :recovering | :up | :delayed | :resuming

  @type recovery_runtime :: %{
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
          recovery: Recovery.t() | nil,
          last_recovery_at: integer() | nil,
          recovery_runtime: recovery_runtime() | nil
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
    :last_recovery_at,
    :recovery_runtime,
    status: :down
  ]

  @doc false
  @spec configure_recovery(t(), keyword()) :: t()
  def configure_recovery(%__MODULE__{} = producer, opts) do
    runtime = %{
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

    %{producer | recovery_runtime: runtime}
  end

  @doc false
  @spec public(t()) :: t()
  def public(%__MODULE__{recovery: %Recovery{}} = producer) do
    %{
      producer
      | status: :recovering,
        processing_queue_delay: nil,
        recovery: nil,
        last_recovery_at: nil,
        recovery_runtime: nil
    }
  end

  def public(%__MODULE__{} = producer) do
    %{producer | recovery: nil, last_recovery_at: nil, recovery_runtime: nil}
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
  def require_recovery(%__MODULE__{} = producer, after_ts) do
    %{producer | recovery: Recovery.new(after_ts)}
  end

  @doc false
  @spec replace_recovery(t(), integer() | nil) :: t()
  def replace_recovery(%__MODULE__{recovery: %Recovery{} = recovery} = producer, after_ts) do
    %{producer | recovery: Recovery.pending(recovery, after_ts)}
  end

  @spec retry_recovery(t()) :: t()
  defp retry_recovery(%__MODULE__{recovery: %Recovery{} = recovery} = producer) do
    %{producer | recovery: Recovery.pending(recovery)}
  end

  @spec mark_recovery_issued(t(), integer(), integer()) :: t()
  defp mark_recovery_issued(%__MODULE__{recovery: %Recovery{} = recovery} = producer, request_id, monotonic_now) do
    %{
      producer
      | recovery: Recovery.in_flight(recovery, request_id),
        last_recovery_at: monotonic_now
    }
  end

  @doc false
  @spec complete_recovery(t(), integer(), integer()) :: {:ok, t()} | :stale
  def complete_recovery(%__MODULE__{recovery: %Recovery{} = recovery} = producer, request_id, now) do
    if Recovery.matches_request?(recovery, request_id) do
      {:ok,
       %{
         producer
         | recovery: nil,
           status: :up,
           last_alive_at: now,
           processing_queue_delay: nil
       }}
    else
      :stale
    end
  end

  def complete_recovery(%__MODULE__{}, _request_id, _now), do: :stale

  @doc false
  @spec park_recovery(t()) :: t()
  def park_recovery(%__MODULE__{recovery: %Recovery{} = recovery} = producer) do
    if Recovery.pending?(recovery), do: producer, else: retry_recovery(producer)
  end

  def park_recovery(%__MODULE__{} = producer), do: producer

  @doc false
  @spec recovering?(t()) :: boolean()
  def recovering?(%__MODULE__{recovery: %Recovery{}}), do: true
  def recovering?(%__MODULE__{}), do: false

  @doc false
  @spec recovery_pending?(t()) :: boolean()
  def recovery_pending?(%__MODULE__{recovery: %Recovery{} = recovery}), do: Recovery.pending?(recovery)
  def recovery_pending?(%__MODULE__{}), do: false

  @spec recovery_generation_matches?(t(), reference()) :: boolean()
  defp recovery_generation_matches?(%__MODULE__{recovery: %Recovery{} = recovery}, generation) do
    Recovery.matches_generation?(recovery, generation)
  end

  defp recovery_generation_matches?(%__MODULE__{}, _generation), do: false

  @spec recovery_request_matches?(t(), integer()) :: boolean()
  defp recovery_request_matches?(%__MODULE__{recovery: %Recovery{} = recovery}, request_id) do
    Recovery.matches_request?(recovery, request_id)
  end

  defp recovery_request_matches?(%__MODULE__{}, _request_id), do: false

  @spec recovery_generation(t()) :: reference()
  defp recovery_generation(%__MODULE__{recovery: %Recovery{generation: generation}}), do: generation

  @spec recovery_after(t()) :: integer() | nil
  defp recovery_after(%__MODULE__{recovery: %Recovery{after_ts: after_ts}}), do: after_ts

  @spec recovery_cooldown(t(), integer(), non_neg_integer()) :: non_neg_integer()
  defp recovery_cooldown(%__MODULE__{last_recovery_at: nil}, _monotonic_now, _min_interval_ms), do: 0

  defp recovery_cooldown(%__MODULE__{last_recovery_at: last}, monotonic_now, min_interval_ms) do
    max(min_interval_ms - (monotonic_now - last), 0)
  end

  @doc false
  @spec initiate_recovery(t(), boolean()) :: t()
  def initiate_recovery(%__MODULE__{recovery_runtime: runtime} = producer, active?) do
    case recovery_cooldown(producer, runtime.monotonic_fun.(), runtime.min_interval_ms) do
      0 -> issue_recovery(producer, active?)
      remaining -> defer_recovery(producer, remaining)
    end
  end

  @doc false
  @spec handle_retry(t(), reference(), boolean()) :: t()
  def handle_retry(%__MODULE__{} = producer, generation, active?) do
    if recovery_generation_matches?(producer, generation) do
      issue_recovery(producer, active?)
    else
      producer
    end
  end

  @doc false
  @spec handle_stall(t(), integer(), boolean()) :: t()
  def handle_stall(%__MODULE__{} = producer, request_id, active?) do
    if recovery_request_matches?(producer, request_id) do
      Logger.warning("UOF.SDK.ProducerMonitor: producer #{producer.id} recovery #{request_id} stalled; reissuing")

      producer |> retry_recovery() |> issue_recovery(active?)
    else
      producer
    end
  end

  defp defer_recovery(producer, remaining) do
    Logger.info("UOF.SDK.ProducerMonitor: producer #{producer.id} recovery deferred for #{remaining}ms")
    schedule({:retry, producer.id, recovery_generation(producer)}, remaining)
    producer
  end

  defp issue_recovery(producer, false) do
    Logger.info("UOF.SDK.ProducerMonitor: producer #{producer.id} recovery parked: control plane passive")
    producer
  end

  defp issue_recovery(%__MODULE__{recovery_runtime: runtime} = producer, true) do
    after_ts = recovery_after(producer)
    request_id = runtime.gen_request_id.()
    opts = build_opts(after_ts, request_id, runtime.node_id)
    producer = mark_recovery_issued(producer, request_id, runtime.monotonic_fun.())

    case safe_recover(runtime.request, producer.product, opts) do
      :ok ->
        emit_initiated(producer, request_id, after_ts)
        schedule({:stall, producer.id, request_id}, runtime.max_recovery_ms)
        producer

      :error ->
        Logger.warning(
          "UOF.SDK.ProducerMonitor: producer #{producer.id} recovery request failed; " <>
            "retrying in #{runtime.min_interval_ms}ms"
        )

        producer = retry_recovery(producer)
        schedule({:retry, producer.id, recovery_generation(producer)}, runtime.min_interval_ms)
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

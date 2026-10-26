defmodule UOF.SDK.ProducerMonitor.Requests do
  @moduledoc """
  Sends recovery requests in tasks linked to the producer monitor.

  Holds shared recovery settings, the request-ID sequence, and the monotonic
  clock. `UOF.SDK.ProducerMonitor.Producer` decides when a request is due.
  Results return as `{:recovery_response, producer_id, request_id, result}`.
  """

  alias UOF.Schemas.Common.Response
  alias UOF.SDK.ProducerMonitor.Producer

  require Logger

  @default_min_interval_ms 30_000
  @default_max_recovery_ms 10 * 60_000
  @max_request_id 1_000_000
  @recovery_overlap_ms 60_000
  @recovery_window_margin_ms 10 * 60_000

  @type t :: %__MODULE__{
          recover_fun: (term(), keyword() -> term()),
          node_id: integer() | nil,
          min_interval_ms: non_neg_integer(),
          max_recovery_ms: pos_integer(),
          monotonic_fun: (-> integer()),
          next_request_id: pos_integer()
        }

  @enforce_keys [:recover_fun, :node_id, :min_interval_ms, :max_recovery_ms, :monotonic_fun, :next_request_id]
  defstruct @enforce_keys

  @doc false
  @spec new(keyword()) :: t()
  def new(opts) do
    %__MODULE__{
      recover_fun: Keyword.get(opts, :recover_fun, &UOF.API.Recovery.recover/2),
      node_id: Keyword.get(opts, :node_id),
      min_interval_ms: Keyword.get(opts, :min_interval_ms, @default_min_interval_ms),
      max_recovery_ms: Keyword.get(opts, :max_recovery_ms, @default_max_recovery_ms),
      monotonic_fun: Keyword.get(opts, :monotonic_fun, fn -> System.monotonic_time(:millisecond) end),
      next_request_id: Keyword.get_lazy(opts, :request_id_start, fn -> :rand.uniform(@max_request_id) end)
    }
  end

  @doc """
  Send the producer's recovery request if it is due, after dropping a stalled
  one. Called by the monitor only while it owns the control plane.
  """
  @spec advance(t(), Producer.t(), integer(), non_neg_integer()) :: {t(), Producer.t()}
  def advance(%__MODULE__{} = requests, %Producer{} = producer, now, inactivity_ms) do
    monotonic_now = requests.monotonic_fun.()
    producer = Producer.drop_stalled_request(producer, monotonic_now, requests.max_recovery_ms)

    if Producer.due?(producer, now, monotonic_now, inactivity_ms, requests.min_interval_ms),
      do: send_request(requests, producer, now, monotonic_now),
      else: {requests, producer}
  end

  @doc false
  @spec handle_response(t(), Producer.t(), integer(), :ok | {:error, term()}) :: Producer.t()
  def handle_response(%__MODULE__{} = requests, %Producer{} = producer, request_id, result) do
    Producer.handle_response(producer, request_id, result, requests.monotonic_fun.(), requests.min_interval_ms)
  end

  @doc false
  @spec complete(t(), Producer.t(), integer(), integer()) :: Producer.t()
  def complete(%__MODULE__{} = requests, %Producer{} = producer, request_id, now) do
    Producer.complete_recovery(producer, request_id, now, requests.monotonic_fun.())
  end

  # Link HTTP tasks so exceptions crash the monitor without blocking observations.
  # A superseded request may already have reached Betradar; let its task finish.
  defp send_request(requests, producer, now, monotonic_now) do
    after_ts = recovery_after(Producer.replay_from(producer), producer.recovery_window_minutes, now)
    request_id = requests.next_request_id
    opts = build_opts(after_ts, request_id, requests.node_id)
    recover_fun = requests.recover_fun
    %Producer{id: id, product: product} = producer
    Task.async(fn -> {:recovery_response, id, request_id, recover(recover_fun, product, opts)} end)

    {%{requests | next_request_id: next_request_id(request_id)},
     Producer.start_request(producer, request_id, after_ts, monotonic_now)}
  end

  # A successful HTTP status may still contain a rejection; no completion follows
  # it. Expected API failures return errors; exceptions crash the monitor.
  defp recover(recover_fun, product, opts) do
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

  # Randomize the starting ID to reduce collisions with recoveries from
  # previous monitor instances whose completions may still be queued.
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
end

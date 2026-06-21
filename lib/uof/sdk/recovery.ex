defmodule UOF.SDK.Recovery do
  @moduledoc """
  Per-producer recovery orchestration.

  `ProducerMonitor` calls `request/2` when a producer needs to be brought back
  in sync. This GenServer then:

    * computes the `after:` timestamp from `UOF.SDK.CheckpointStore` (clamped to
      the producer's `recovery_window_minutes`; a full recovery when there is no
      checkpoint),
    * issues `UOF.API.Recovery.recover/2` with a fresh `request_id` and the
      configured `node_id`, emitting a `[:uof_sdk, :recovery, :initiated]`
      telemetry event (the hook for spotting infinite-recovery loops),
    * keeps at most one in-flight recovery per producer,
    * **reissues** with the *original* timestamp if no `snapshot_complete`
      arrives within `max_recovery_ms` (the stall guard),
    * **retries** after `min_interval_ms` if the API call itself fails, and
    * on the matching `snapshot_complete`, tells `ProducerMonitor` the recovery
      completed.

  The defaults for `min_interval_ms` / `max_recovery_ms` mirror the official
  SDK and are throttling-safe; change with care (see Betradar's recovery docs on
  infinite-recovery loops).
  """

  use GenServer

  require Logger

  alias UOF.SDK.{Producer, ProducerMonitor}

  @default_min_interval_ms 30_000
  @default_max_recovery_ms 5 * 60_000

  ## Client API --------------------------------------------------------------

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc "Request recovery for a producer (idempotent while one is in flight)."
  def request(server \\ __MODULE__, %Producer{} = producer) do
    GenServer.cast(server, {:request, producer})
  end

  @doc "Feed a `snapshot_complete` for correlation against the in-flight recovery."
  def snapshot_complete(server \\ __MODULE__, producer_id, request_id) do
    GenServer.cast(server, {:snapshot_complete, producer_id, request_id})
  end

  ## Server ------------------------------------------------------------------

  @impl true
  def init(opts) do
    monitor = Keyword.get(opts, :monitor, ProducerMonitor)

    state = %{
      recover_fun: Keyword.get(opts, :recover_fun, &UOF.API.Recovery.recover/2),
      checkpoint_store: Keyword.get(opts, :checkpoint_store, UOF.SDK.CheckpointStore.ETS),
      node_id: Keyword.get(opts, :node_id),
      min_interval_ms: Keyword.get(opts, :min_interval_ms, @default_min_interval_ms),
      max_recovery_ms: Keyword.get(opts, :max_recovery_ms, @default_max_recovery_ms),
      now_fun: Keyword.get(opts, :now_fun, fn -> System.system_time(:millisecond) end),
      gen_request_id:
        Keyword.get(opts, :gen_request_id, fn -> System.unique_integer([:positive, :monotonic]) end),
      on_complete:
        Keyword.get(opts, :on_complete, fn id, rid ->
          ProducerMonitor.recovery_completed(monitor, id, rid)
        end),
      recoveries: %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_cast({:request, producer}, state) do
    if Map.has_key?(state.recoveries, producer.id) do
      {:noreply, state}
    else
      {:noreply, initiate(state, producer, after_from_checkpoint(state, producer))}
    end
  end

  def handle_cast({:snapshot_complete, id, request_id}, state) do
    case Map.get(state.recoveries, id) do
      %{request_id: ^request_id} ->
        Logger.info("UOF.SDK.Recovery: producer #{id} recovery #{request_id} complete")
        state.on_complete.(id, request_id)
        {:noreply, %{state | recoveries: Map.delete(state.recoveries, id)}}

      _other ->
        # stale request id, unknown producer, or another node's completion
        {:noreply, state}
    end
  end

  @impl true
  def handle_call(:sync, _from, state), do: {:reply, :ok, state}

  @impl true
  def handle_info({:stall, id, request_id}, state) do
    case Map.get(state.recoveries, id) do
      %{request_id: ^request_id, after_ts: after_ts, producer: producer} ->
        Logger.warning("UOF.SDK.Recovery: producer #{id} recovery #{request_id} stalled; reissuing")
        {:noreply, initiate(state, producer, after_ts)}

      _other ->
        {:noreply, state}
    end
  end

  def handle_info({:retry, producer}, state) do
    if Map.has_key?(state.recoveries, producer.id) do
      {:noreply, state}
    else
      {:noreply, initiate(state, producer, after_from_checkpoint(state, producer))}
    end
  end

  ## internals ---------------------------------------------------------------

  defp initiate(state, producer, after_ts) do
    request_id = state.gen_request_id.()
    opts = build_opts(after_ts, request_id, state.node_id)

    case safe_recover(state, producer.product, opts) do
      :ok ->
        emit_initiated(producer, request_id, after_ts)
        Process.send_after(self(), {:stall, producer.id, request_id}, state.max_recovery_ms)
        recovery = %{request_id: request_id, after_ts: after_ts, producer: producer}
        %{state | recoveries: Map.put(state.recoveries, producer.id, recovery)}

      :error ->
        Logger.warning(
          "UOF.SDK.Recovery: producer #{producer.id} recovery request failed; " <>
            "retrying in #{state.min_interval_ms}ms"
        )

        Process.send_after(self(), {:retry, producer}, state.min_interval_ms)
        state
    end
  end

  defp safe_recover(state, product, opts) do
    case state.recover_fun.(product, opts) do
      {:ok, _response} -> :ok
      {:error, reason} -> log_failure(inspect(reason))
    end
  rescue
    exception -> log_failure(Exception.message(exception))
  end

  defp log_failure(detail) do
    Logger.warning("UOF.SDK.Recovery: recover request failed: #{detail}")
    :error
  end

  defp after_from_checkpoint(state, producer) do
    case state.checkpoint_store.get(producer.id) do
      {:ok, timestamp} -> clamp_to_window(timestamp, producer, state.now_fun.())
      :none -> nil
    end
  end

  defp clamp_to_window(timestamp, %Producer{recovery_window_minutes: nil}, _now), do: timestamp

  defp clamp_to_window(timestamp, %Producer{recovery_window_minutes: minutes}, now) do
    earliest = now - minutes * 60_000
    max(timestamp, earliest)
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
      "UOF.SDK.Recovery: producer #{producer.id} (#{producer.product}) recovery initiated " <>
        "request_id=#{request_id} after=#{inspect(after_ts)}"
    )
  end
end

defmodule UOF.SDK.ProducerMonitorTest do
  use ExUnit.Case, async: false

  alias UOF.SDK.MonitorSnapshot
  alias UOF.SDK.MonitorStore
  alias UOF.SDK.Producer
  alias UOF.SDK.ProducerMonitor

  @inactivity 10_000
  @now 1_000_000_000_000

  defmodule Handler do
    @moduledoc false
    use UOF.SDK.MessageHandler

    @impl true
    def handle_producer_status(producer) do
      send(Application.fetch_env!(:uof_sdk, :test_pid), {:status, producer})
      :ok
    end
  end

  setup do
    Application.put_env(:uof_sdk, :test_pid, self())
    start_supervised!(MonitorStore.ETS)
    clock = start_supervised!(%{id: :clock, start: {Agent, :start_link, [fn -> 1_000 end]}})
    %{clock: clock}
  end

  # Starts a monitor whose recover_fun signals the test with the request_id.
  defp start_monitor(overrides \\ [], clock) do
    test_pid = self()

    defaults = [
      producers: [%Producer{id: 1, product: "pre"}],
      handler: Handler,
      recover_fun: fn product, opts ->
        send(test_pid, {:recover_called, product, opts})
        {:ok, :accepted}
      end,
      now_fun: fn -> Agent.get(clock, & &1) end,
      inactivity_ms: @inactivity,
      max_processing_delay_ms: @inactivity,
      tick_ms: 60_000,
      recovery_overlap_ms: 0
    ]

    start_supervised!({ProducerMonitor, Keyword.merge(defaults, overrides)})
  end

  defp set_clock(clock, value), do: Agent.update(clock, fn _ -> value end)

  defp put_checkpoint(id, timestamp) do
    snapshot = MonitorStore.ETS.load()
    MonitorStore.ETS.save(MonitorSnapshot.put_checkpoint(snapshot, id, timestamp))
  end

  defp checkpoint(id) do
    case MonitorSnapshot.checkpoint(MonitorStore.ETS.load(), id) do
      nil -> :none
      timestamp -> {:ok, timestamp}
    end
  end

  defp save_snapshot(checkpoints, resumable_producers, connection_tokens \\ %{}) do
    MonitorStore.ETS.save(%MonitorSnapshot{
      checkpoints: checkpoints,
      resumable_producers: MapSet.new(resumable_producers),
      connection_tokens: connection_tokens
    })
  end

  defp tick(monitor) do
    send(monitor, :tick)
    GenServer.call(monitor, :sync)
  end

  defp sync(monitor), do: GenServer.call(monitor, :sync)

  defp assert_recovery_triggered(product \\ "pre") do
    assert_receive {:recover_called, ^product, opts}
    opts[:request_id]
  end

  ## Health monitoring ---------------------------------------------------------

  test "first alive triggers recovery; snapshot_complete brings producer up", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    sync(m)
    assert {:ok, %Producer{status: :recovering}} = ProducerMonitor.producer(m, 1)

    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %Producer{id: 1, status: :up}}
  end

  test "checkpoints subscribed alive timestamps only after producer is up", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    sync(m)
    assert checkpoint(1) == :none

    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %Producer{status: :up}}

    ProducerMonitor.alive(m, 1, 2_000, true)
    sync(m)
    assert checkpoint(1) == {:ok, 2_000}

    ProducerMonitor.alive(m, 1, 1_500, true)
    sync(m)
    assert checkpoint(1) == {:ok, 2_000}
  end

  test "does not checkpoint unsubscribed alive timestamps", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, false)
    assert_recovery_triggered()
    sync(m)

    assert checkpoint(1) == :none
  end

  test "silence (alive interval violation) marks down and recovers", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid1 = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid1)
    assert_receive {:status, %Producer{status: :up}}

    # advance past inactivity with no new alive
    set_clock(clock, 1_000 + @inactivity + 1)
    tick(m)

    assert_receive {:status, %Producer{status: :recovering}}
    rid2 = assert_recovery_triggered()

    # A later recovery follows the same status lifecycle.
    ProducerMonitor.snapshot_complete(m, 1, rid2)
    assert_receive {:status, %Producer{status: :up}}
  end

  test "processing lag marks down + delayed without recovering", %{clock: clock} do
    m = start_monitor(clock)

    # bring it up first
    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %Producer{status: :up}}

    # Local clock jumps to 20_000, but the newest message the content pipeline
    # processed was generated back at 1_000. A fresh system alive keeps the
    # alive-interval path quiet, so the processing check is what trips.
    set_clock(clock, 20_000)
    ProducerMonitor.alive(m, 1, 20_000, true)
    ProducerMonitor.message(m, 1, 1_000)
    sync(m)

    tick(m)
    assert_receive {:status, %Producer{status: :delayed}}

    refute_received {:recover_called, _, _}

    # content progress generated at ~now arrives -> caught up -> stabilized
    ProducerMonitor.message(m, 1, 20_000)
    tick(m)

    assert_receive {:status, %Producer{status: :up}}
  end

  test "content-session alive freshness prevents quiet producers from being marked delayed", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %Producer{status: :up}}

    set_clock(clock, 20_000)

    # System alive keeps delivery health/checkpointing fresh. The content
    # session's own alive is queued behind content and therefore proves local
    # content processing is not lagging, even when there are no event messages.
    ProducerMonitor.alive(m, 1, 20_000, true)
    ProducerMonitor.message(m, 1, 20_000)
    tick(m)

    refute_received {:status, %Producer{status: :delayed}}
    refute_received {:recover_called, _, _}
    assert {:ok, %Producer{status: :up}} = ProducerMonitor.producer(m, 1)
  end

  test "content-session alive freshness does not write checkpoints", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %Producer{status: :up}}

    ProducerMonitor.message(m, 1, 2_000)
    sync(m)

    assert checkpoint(1) == :none
  end

  test "alive on a delayed producer does not trigger recovery", %{clock: clock} do
    m = start_monitor(clock)

    # bring it up
    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %Producer{status: :up}}

    # drive it into processing lag: local clock at 20_000 but
    # content progress is still back at 1_000, with a fresh alive so the
    # alive-interval path stays quiet and the processing check is what trips.
    set_clock(clock, 20_000)
    ProducerMonitor.alive(m, 1, 20_000, true)
    ProducerMonitor.message(m, 1, 1_000)
    sync(m)
    tick(m)
    assert_receive {:status, %Producer{status: :delayed}}

    # another subscribed alive arrives while still delayed and still behind: the
    # remote feed is healthy, so this must NOT issue a recovery (the flop bug
    # re-triggered on every alive).
    ProducerMonitor.alive(m, 1, 1_000, true)
    sync(m)
    refute_received {:recover_called, _, _}
    assert {:ok, %Producer{status: :delayed}} = ProducerMonitor.producer(m, 1)
  end

  test "startup connection recovery waits for both system and content namespaces", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, :conn_a})
    sync(m)
    refute_received {:recover_called, _, _}

    ProducerMonitor.observe_connection(m, {:content, :conn_a})
    rid1 = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid1)
    assert_receive {:status, %Producer{status: :up}}

    # same connection token -> deduped, no recovery
    ProducerMonitor.observe_connection(m, {:system, :conn_a})
    sync(m)
    refute_received {:recover_called, _, _}

    # new connection token (a reconnect) -> down + recover
    ProducerMonitor.observe_connection(m, {:system, :conn_b})
    assert_receive {:status, %Producer{status: :recovering}}
    assert_receive {:recover_called, "pre", _}
  end

  test "startup alive does not recover while only one connection namespace is ready", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, :conn_a})
    ProducerMonitor.alive(m, 1, 1_000, true)
    sync(m)

    refute_received {:recover_called, _, _}
    assert {:ok, %Producer{status: :down}} = ProducerMonitor.producer(m, 1)

    ProducerMonitor.observe_connection(m, {:content, :conn_a})
    assert_receive {:status, %Producer{status: :recovering}}
    assert_receive {:recover_called, "pre", _}
  end

  test "startup alive interval violation does not recover while only one connection namespace is ready", %{
    clock: clock
  } do
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, :conn_a})
    ProducerMonitor.alive(m, 1, 1_000, true)
    sync(m)

    set_clock(clock, 1_000 + @inactivity + 1)
    tick(m)

    refute_received {:recover_called, _, _}
    assert {:ok, %Producer{status: :down}} = ProducerMonitor.producer(m, 1)

    ProducerMonitor.observe_connection(m, {:content, :conn_a})
    assert_receive {:status, %Producer{status: :recovering}}
    assert_receive {:recover_called, "pre", _}
  end

  test "first token from the second connection namespace triggers one startup recovery", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, :same_underlying_token})
    sync(m)
    refute_received {:status, %Producer{status: :recovering}}
    refute_received {:recover_called, _, _}

    ProducerMonitor.observe_connection(m, {:content, :same_underlying_token})
    assert_receive {:status, %Producer{status: :recovering}}
    rid1 = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid1)
    assert_receive {:status, %Producer{status: :up}}
  end

  test "same namespace token changes before startup is ready do not recover early", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, :system_a})
    ProducerMonitor.observe_connection(m, {:system, :system_b})
    sync(m)
    refute_received {:recover_called, _, _}

    ProducerMonitor.observe_connection(m, {:content, :content_a})
    assert_receive {:status, %Producer{status: :recovering}}
    assert_receive {:recover_called, "pre", _}
  end

  test "startup gate requires system and content namespaces explicitly", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:default, :default_a})
    ProducerMonitor.observe_connection(m, {:system, :system_a})
    sync(m)
    refute_received {:recover_called, _, _}

    ProducerMonitor.observe_connection(m, {:content, :content_a})
    assert_receive {:status, %Producer{status: :recovering}}
    assert_receive {:recover_called, "pre", _}
  end

  test "token changes in either connection namespace trigger recovery after startup", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, :system_a})
    sync(m)
    refute_received {:recover_called, _, _}

    ProducerMonitor.observe_connection(m, {:content, :content_a})
    assert_receive {:status, %Producer{status: :recovering}}
    rid1 = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid1)
    assert_receive {:status, %Producer{status: :up}}

    ProducerMonitor.observe_connection(m, {:content, :content_b})
    assert_receive {:status, %Producer{status: :recovering}}
    rid2 = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid2)
    assert_receive {:status, %Producer{status: :up}}

    ProducerMonitor.observe_connection(m, {:system, :system_b})
    assert_receive {:status, %Producer{status: :recovering}}
    assert_receive {:recover_called, "pre", _}
  end

  test "producers/1 returns all producers ordered by id", %{clock: clock} do
    m =
      start_monitor(
        [producers: [%Producer{id: 3, product: "pre"}, %Producer{id: 1, product: "liveodds"}]],
        clock
      )

    assert [%Producer{id: 1}, %Producer{id: 3}] = ProducerMonitor.producers(m)
  end

  test "producer/2 returns :error for unknown id", %{clock: clock} do
    m = start_monitor(clock)
    assert :error = ProducerMonitor.producer(m, 99)
  end

  ## Recovery orchestration ----------------------------------------------------

  test "full recovery (no checkpoint) omits :after", %{clock: clock} do
    m = start_monitor(clock)
    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_receive {:recover_called, "pre", opts}
    refute Keyword.has_key?(opts, :after)
  end

  test "incremental recovery uses the checkpoint as :after", %{clock: clock} do
    put_checkpoint(1, @now - 60_000)
    m = start_monitor([now_fun: fn -> @now end], clock)
    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_receive {:recover_called, "pre", opts}
    assert opts[:after] == @now - 60_000
  end

  test "incremental recovery subtracts configured overlap from checkpoint", %{clock: clock} do
    put_checkpoint(1, @now - 60_000)

    m =
      start_monitor(
        [
          now_fun: fn -> @now end,
          recovery_overlap_ms: 30_000
        ],
        clock
      )

    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_receive {:recover_called, "pre", opts}
    assert opts[:after] == @now - 90_000
  end

  test "clamps :after to the producer's recovery window", %{clock: clock} do
    # checkpoint is 2 h old, window is 60 min -> clamp to now - 60 min
    put_checkpoint(1, @now - 2 * 60 * 60_000)

    m =
      start_monitor(
        [
          producers: [%Producer{id: 1, product: "pre", recovery_window_minutes: 60}],
          now_fun: fn -> @now end
        ],
        clock
      )

    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_receive {:recover_called, "pre", opts}
    assert opts[:after] == @now - 60 * 60_000
  end

  test "keeps a single in-flight recovery per producer", %{clock: clock} do
    m = start_monitor(clock)
    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_receive {:recover_called, "pre", _}

    # second alive while one is already in flight -> no new request
    ProducerMonitor.alive(m, 1, 1_000, true)
    sync(m)
    refute_received {:recover_called, _, _}
  end

  test "non-matching snapshot_complete is ignored", %{clock: clock} do
    m = start_monitor(clock)
    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_receive {:recover_called, "pre", _}
    assert_receive {:status, %Producer{status: :recovering}}

    ProducerMonitor.snapshot_complete(m, 1, 9_999)
    sync(m)
    refute_received {:status, _}
    assert {:ok, %Producer{status: :recovering}} = ProducerMonitor.producer(m, 1)
  end

  test "manual recover goes through the full lifecycle", %{clock: clock} do
    put_checkpoint(1, @now - 60_000)
    m = start_monitor([now_fun: fn -> @now end], clock)

    assert :ok = ProducerMonitor.recover(m, 1, [])
    assert_receive {:status, %Producer{status: :recovering}}
    assert_receive {:recover_called, "pre", opts}
    assert opts[:after] == @now - 60_000

    # a second trigger while one is in flight is refused
    assert {:error, :already_recovering} = ProducerMonitor.recover(m, 1, [])

    # and the completion correlates like any automatic recovery
    ProducerMonitor.snapshot_complete(m, 1, opts[:request_id])
    assert_receive {:status, %Producer{status: :up}}
  end

  test "manual full recover ignores the checkpoint", %{clock: clock} do
    put_checkpoint(1, @now - 60_000)
    m = start_monitor([now_fun: fn -> @now end], clock)

    assert :ok = ProducerMonitor.recover(m, 1, full: true)
    assert_receive {:recover_called, "pre", opts}
    refute Keyword.has_key?(opts, :after)

    assert {:error, :unknown_producer} = ProducerMonitor.recover(m, 99, full: true)
  end

  test "manual recovery completion establishes the alive timeout anchor", %{clock: clock} do
    m = start_monitor(clock)

    assert :ok = ProducerMonitor.recover(m, 1, full: true)
    assert_receive {:recover_called, "pre", opts}

    ProducerMonitor.snapshot_complete(m, 1, opts[:request_id])
    assert_receive {:status, %Producer{status: :up, last_alive_at: 1_000}}

    set_clock(clock, 1_000 + @inactivity + 1)
    tick(m)
    assert_receive {:status, %Producer{status: :recovering}}
    assert_receive {:recover_called, "pre", _}
  end

  test "API failure schedules a retry", %{clock: clock} do
    test_pid = self()
    attempts = start_supervised!(%{id: :attempts, start: {Agent, :start_link, [fn -> 0 end]}})

    flaky = fn product, opts ->
      n = Agent.get_and_update(attempts, fn n -> {n, n + 1} end)
      send(test_pid, {:recover_called, product, opts})
      if n == 0, do: {:error, :boom}, else: {:ok, :accepted}
    end

    m = start_monitor([recover_fun: flaky, min_interval_ms: 10], clock)
    ProducerMonitor.alive(m, 1, 1_000, true)

    assert_receive {:recover_called, "pre", _}, 200
    assert_receive {:recover_called, "pre", _}, 500
    assert Agent.get(attempts, & &1) >= 2
  end

  test "API failure retry preserves the original :after timestamp", %{clock: clock} do
    put_checkpoint(1, @now - 60_000)

    test_pid = self()
    attempts = start_supervised!(%{id: :attempts_for_retry_after, start: {Agent, :start_link, [fn -> 0 end]}})

    flaky = fn product, opts ->
      n = Agent.get_and_update(attempts, fn n -> {n, n + 1} end)
      send(test_pid, {:recover_called, product, opts})

      if n == 0 do
        put_checkpoint(1, @now)
        {:error, :boom}
      else
        {:ok, :accepted}
      end
    end

    m = start_monitor([recover_fun: flaky, min_interval_ms: 10, now_fun: fn -> @now end], clock)
    ProducerMonitor.alive(m, 1, 1_000, true)

    assert_receive {:recover_called, "pre", first}, 200
    assert_receive {:recover_called, "pre", second}, 500

    assert first[:after] == @now - 60_000
    assert second[:after] == first[:after]
  end

  test "a rejection response envelope schedules a retry", %{clock: clock} do
    test_pid = self()
    attempts = start_supervised!(%{id: :attempts, start: {Agent, :start_link, [fn -> 0 end]}})

    throttled = fn product, opts ->
      n = Agent.get_and_update(attempts, fn n -> {n, n + 1} end)
      send(test_pid, {:recover_called, product, opts})

      response_code = if n == 0, do: "FORBIDDEN", else: "ACCEPTED"
      {:ok, %UOF.Schemas.Common.Response{response_code: response_code, message: "max requests exceeded"}}
    end

    m = start_monitor([recover_fun: throttled, min_interval_ms: 10], clock)
    ProducerMonitor.alive(m, 1, 1_000, true)

    # rejected request must not count as in-flight: no snapshot_complete is
    # coming, so a retry (new request_id) has to be issued
    assert_receive {:recover_called, "pre", first}, 200
    assert_receive {:recover_called, "pre", second}, 500
    assert second[:request_id] != first[:request_id]

    # the accepted retry is the one snapshot_complete correlates against
    ProducerMonitor.snapshot_complete(m, 1, second[:request_id])
    assert_receive {:status, %Producer{id: 1, status: :up}}
  end

  test "stall guard reissues with the original :after timestamp", %{clock: clock} do
    put_checkpoint(1, @now - 60_000)
    m = start_monitor([now_fun: fn -> @now end, max_recovery_ms: 20], clock)
    ProducerMonitor.alive(m, 1, 1_000, true)

    assert_receive {:recover_called, "pre", first}
    assert_receive {:recover_called, "pre", second}, 500
    assert second[:after] == first[:after]
    assert second[:request_id] != first[:request_id]
  end

  ## Restart resume -------------------------------------------------------------

  # Persist the state of a producer that was up, with a checkpoint and both
  # namespace tokens — the shape a healthy shutdown leaves behind.
  defp persist_healthy_shutdown(checkpoint) do
    save_snapshot(
      %{1 => checkpoint},
      [1],
      %{system: "ctag-sys", content: "ctag-con"}
    )
  end

  test "resumed producer skips startup recovery and returns up once caught up", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    set_clock(clock, 50_000)
    m = start_monitor(clock)

    # same tokens after restart -> same upstream consume session -> no recovery
    ProducerMonitor.observe_connection(m, {:system, "ctag-sys"})
    ProducerMonitor.observe_connection(m, {:content, "ctag-con"})
    # subscribed alive on the resuming producer -> no recovery either
    ProducerMonitor.alive(m, 1, 50_000, true)
    sync(m)
    refute_received {:recover_called, _, _}
    assert {:ok, %Producer{status: :resuming}} = ProducerMonitor.producer(m, 1)

    # still draining: last_message_timestamp is seeded from the checkpoint
    tick(m)
    refute_received {:status, %Producer{status: :up}}

    # backlog catches up -> stabilized -> up, without ever recovering
    ProducerMonitor.message(m, 1, 50_000)
    tick(m)
    assert_receive {:status, %Producer{status: :up}}
    refute_received {:recover_called, _, _}
  end

  test "resumed producer stays down until an alive and both connections are observed", %{clock: clock} do
    # checkpoint is recent, so the processing-lag check alone would not hold the
    # producer down — the heartbeat gate is what must keep it down here
    persist_healthy_shutdown(1_000)
    set_clock(clock, 1_500)
    m = start_monitor(clock)

    tick(m)
    refute_received {:status, %Producer{status: :up}}
    assert {:ok, %Producer{status: :resuming}} = ProducerMonitor.producer(m, 1)

    ProducerMonitor.observe_connection(m, {:system, "ctag-sys"})
    ProducerMonitor.alive(m, 1, 1_600, true)
    sync(m)
    refute_received {:recover_called, _, _}

    tick(m)
    refute_received {:status, %Producer{status: :up}}

    ProducerMonitor.observe_connection(m, {:content, "ctag-con"})
    tick(m)
    assert_receive {:status, %Producer{status: :up}}
  end

  test "resumed producer recovers incrementally when a namespace token changed across restart", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    set_clock(clock, 50_000)
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, "ctag-sys-new"})
    sync(m)
    refute_received {:recover_called, _, _}

    # A persisted content token is only a comparison baseline. Recovery waits
    # until the content pipeline has actually attached in this monitor session.
    ProducerMonitor.observe_connection(m, {:content, "ctag-con"})

    assert_receive {:status, %Producer{status: :recovering}}
    assert_receive {:recover_called, "pre", opts}
    assert opts[:after] == 1_000
  end

  test "resumed producer recovers on an unsubscribed alive", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    set_clock(clock, 50_000)
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, "ctag-sys"})
    ProducerMonitor.observe_connection(m, {:content, "ctag-con"})
    ProducerMonitor.alive(m, 1, 50_000, false)
    assert_recovery_triggered()

    # Starting recovery must replace the resumable persisted state.
    sync(m)
    refute MonitorSnapshot.resumable?(MonitorStore.ETS.load(), 1)
  end

  test "does not resume a producer persisted as plain down", %{clock: clock} do
    save_snapshot(%{1 => 1_000}, [])
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_recovery_triggered()
  end

  test "does not resume without a checkpoint", %{clock: clock} do
    save_snapshot(%{}, [1])
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_recovery_triggered()
  end

  test "resumes a producer persisted as resumable", %{clock: clock} do
    save_snapshot(%{1 => 1_000}, [1])
    set_clock(clock, 50_000)
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 50_000, true)
    sync(m)
    refute_received {:recover_called, _, _}
    assert {:ok, %Producer{status: :resuming}} = ProducerMonitor.producer(m, 1)
  end

  ## State persistence ----------------------------------------------------------

  test "persists whether a producer can resume", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()

    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %Producer{status: :up}}
    assert MonitorSnapshot.resumable?(MonitorStore.ETS.load(), 1)

    # silence -> down + recovering; persisted flattened so a restart re-recovers
    set_clock(clock, 1_000 + @inactivity + 1)
    tick(m)
    assert_receive {:status, %Producer{status: :recovering}}
    refute MonitorSnapshot.resumable?(MonitorStore.ETS.load(), 1)
  end

  test "persists delayed transitions", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %Producer{status: :up}}

    set_clock(clock, 20_000)
    ProducerMonitor.alive(m, 1, 20_000, true)
    ProducerMonitor.message(m, 1, 1_000)
    tick(m)
    assert_receive {:status, %Producer{status: :delayed}}

    assert MonitorSnapshot.resumable?(MonitorStore.ETS.load(), 1)
  end

  test "persists connection tokens as they change", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, "ctag-a"})
    sync(m)
    assert MonitorStore.ETS.load().connection_tokens == %{}

    ProducerMonitor.observe_connection(m, {:content, "ctag-b"})
    sync(m)
    assert MonitorStore.ETS.load().connection_tokens == %{system: "ctag-a", content: "ctag-b"}

    ProducerMonitor.observe_connection(m, {:system, "ctag-c"})
    sync(m)
    assert MonitorStore.ETS.load().connection_tokens == %{system: "ctag-c", content: "ctag-b"}
  end
end

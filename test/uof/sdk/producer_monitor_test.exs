defmodule UOF.SDK.ProducerMonitorTest do
  use ExUnit.Case, async: false

  alias UOF.SDK.CheckpointStore
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
    start_supervised!(CheckpointStore.ETS)
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
      tick_ms: 60_000
    ]

    start_supervised!({ProducerMonitor, Keyword.merge(defaults, overrides)})
  end

  defp set_clock(clock, value), do: Agent.update(clock, fn _ -> value end)

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
    assert {:ok, %Producer{down?: true, recovering?: true}} = ProducerMonitor.producer(m, 1)

    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %Producer{id: 1, down?: false, reason: :first_recovery_completed}}
  end

  test "silence (alive interval violation) marks down and recovers", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid1 = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid1)
    assert_receive {:status, %Producer{down?: false}}

    # advance past inactivity with no new alive
    set_clock(clock, 1_000 + @inactivity + 1)
    tick(m)

    assert_receive {:status, %Producer{down?: true, reason: :alive_interval_violation}}
    rid2 = assert_recovery_triggered()

    # second completion -> returned_from_inactivity, not first_recovery_completed
    ProducerMonitor.snapshot_complete(m, 1, rid2)
    assert_receive {:status, %Producer{down?: false, reason: :returned_from_inactivity}}
  end

  test "processing lag marks down + delayed without recovering", %{clock: clock} do
    m = start_monitor(clock)

    # bring it up first
    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %Producer{down?: false}}

    # Local clock jumps to 20_000, but the newest message we've managed to
    # process was generated back at 1_000 (a backed-up consumer draining a stale
    # alive) -> ~19s behind. last_alive_at stays fresh, so the alive-interval
    # path is quiet and it's the processing check that trips.
    set_clock(clock, 20_000)
    ProducerMonitor.alive(m, 1, 1_000, true)
    sync(m)

    tick(m)
    assert_receive {:status, %Producer{down?: true, delayed?: true, reason: :processing_queue_delay_violation}}

    refute_received {:recover_called, _, _}

    # a message generated at ~now arrives -> caught up -> stabilized, back up
    ProducerMonitor.alive(m, 1, 20_000, true)
    tick(m)

    assert_receive {:status, %Producer{down?: false, delayed?: false, reason: :processing_queue_delay_stabilized}}
  end

  test "alive on a delayed producer does not trigger recovery", %{clock: clock} do
    m = start_monitor(clock)

    # bring it up
    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %Producer{down?: false}}

    # drive it into processing lag (down? + delayed?): local clock at 20_000 but
    # the last processed message was generated at 1_000, with a fresh alive so
    # the alive-interval path stays quiet and the processing check is what trips.
    set_clock(clock, 20_000)
    ProducerMonitor.alive(m, 1, 1_000, true)
    sync(m)
    tick(m)
    assert_receive {:status, %Producer{down?: true, delayed?: true, reason: :processing_queue_delay_violation}}

    # another subscribed alive arrives while still delayed and still behind: the
    # remote feed is healthy, so this must NOT issue a recovery (the flop bug
    # re-triggered on every alive).
    ProducerMonitor.alive(m, 1, 1_000, true)
    sync(m)
    refute_received {:recover_called, _, _}
    assert {:ok, %Producer{recovering?: false, delayed?: true}} = ProducerMonitor.producer(m, 1)
  end

  test "observing a new connection recovers; same connection is deduped", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, :conn_a)
    rid1 = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid1)
    assert_receive {:status, %Producer{down?: false}}

    # same connection token -> deduped, no recovery
    ProducerMonitor.observe_connection(m, :conn_a)
    sync(m)
    refute_received {:recover_called, _, _}

    # new connection token (a reconnect) -> down + recover
    ProducerMonitor.observe_connection(m, :conn_b)
    assert_receive {:status, %Producer{down?: true, reason: :connection_down}}
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
    CheckpointStore.ETS.put(1, @now - 60_000)
    m = start_monitor([now_fun: fn -> @now end], clock)
    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_receive {:recover_called, "pre", opts}
    assert opts[:after] == @now - 60_000
  end

  test "clamps :after to the producer's recovery window", %{clock: clock} do
    # checkpoint is 2 h old, window is 60 min -> clamp to now - 60 min
    CheckpointStore.ETS.put(1, @now - 2 * 60 * 60_000)

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

    ProducerMonitor.snapshot_complete(m, 1, 9_999)
    sync(m)
    refute_received {:status, _}
    assert {:ok, %Producer{recovering?: true}} = ProducerMonitor.producer(m, 1)
  end

  test "manual recover goes through the full lifecycle", %{clock: clock} do
    CheckpointStore.ETS.put(1, @now - 60_000)
    m = start_monitor([now_fun: fn -> @now end], clock)

    assert :ok = ProducerMonitor.recover(m, 1, [])
    assert_receive {:status, %Producer{down?: true, recovering?: true, reason: :other}}
    assert_receive {:recover_called, "pre", opts}
    assert opts[:after] == @now - 60_000

    # a second trigger while one is in flight is refused
    assert {:error, :already_recovering} = ProducerMonitor.recover(m, 1, [])

    # and the completion correlates like any automatic recovery
    ProducerMonitor.snapshot_complete(m, 1, opts[:request_id])
    assert_receive {:status, %Producer{down?: false, reason: :first_recovery_completed}}
  end

  test "manual full recover ignores the checkpoint", %{clock: clock} do
    CheckpointStore.ETS.put(1, @now - 60_000)
    m = start_monitor([now_fun: fn -> @now end], clock)

    assert :ok = ProducerMonitor.recover(m, 1, full: true)
    assert_receive {:recover_called, "pre", opts}
    refute Keyword.has_key?(opts, :after)

    assert {:error, :unknown_producer} = ProducerMonitor.recover(m, 99, full: true)
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
    assert_receive {:status, %Producer{id: 1, down?: false, reason: :first_recovery_completed}}
  end

  test "stall guard reissues with the original :after timestamp", %{clock: clock} do
    CheckpointStore.ETS.put(1, @now - 60_000)
    m = start_monitor([now_fun: fn -> @now end, max_recovery_ms: 20], clock)
    ProducerMonitor.alive(m, 1, 1_000, true)

    assert_receive {:recover_called, "pre", first}
    assert_receive {:recover_called, "pre", second}, 500
    assert second[:after] == first[:after]
    assert second[:request_id] != first[:request_id]
  end
end

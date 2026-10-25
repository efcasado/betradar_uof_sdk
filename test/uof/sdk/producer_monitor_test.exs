defmodule UOF.SDK.ProducerMonitorTest do
  use ExUnit.Case, async: false

  alias UOF.SDK.ProducerMonitor
  alias UOF.SDK.ProducerMonitor.Producer
  alias UOF.SDK.ProducerMonitor.Snapshot
  alias UOF.SDK.ProducerMonitor.Store
  alias UOF.SDK.ProducerMonitor.Store.ETS
  alias UOF.SDK.ProducerStatus

  @inactivity 10_000
  @now 1_000_000_000_000

  # Records each saved snapshot so tests can order persistence against HTTP.
  defmodule RecordingStore do
    @moduledoc false
    @behaviour Store

    @impl true
    def load(_opts), do: ETS.load([])

    @impl true
    def save(snapshot, opts) do
      send(Keyword.fetch!(opts, :pid), {:saved, Snapshot.decode!(snapshot)})
      ETS.save(snapshot, [])
    end
  end

  defmodule FailingStore do
    @moduledoc false
    @behaviour Store

    @impl true
    def load(_opts), do: ETS.load([])

    @impl true
    def save(_snapshot, _opts), do: raise("store unavailable")
  end

  defmodule Handler do
    @moduledoc false
    use UOF.SDK.MessageHandler

    @impl true
    def handle_producer_status(producer) do
      send(Application.fetch_env!(:uof_sdk_test, :test_pid), {:status, producer})
      :ok
    end
  end

  setup do
    Application.put_env(:uof_sdk_test, :test_pid, self())
    start_supervised!(ETS)
    clock = start_supervised!(%{id: :clock, start: {Agent, :start_link, [fn -> 1_000 end]}})
    %{clock: clock}
  end

  # Starts a monitor whose recover_fun signals the test with the request_id.
  # `producers:` are served by a stubbed descriptions API.
  defp start_monitor(overrides \\ [], clock) do
    test_pid = self()
    {producers, overrides} = Keyword.pop(overrides, :producers, [%Producer{id: 1, product: "pre"}])

    defaults = [
      producer_fetcher: fn -> {:ok, %{producer: Enum.map(producers, &api_description/1)}} end,
      handler: Handler,
      recover_fun: fn product, opts ->
        send(test_pid, {:recover_called, product, opts})
        {:ok, :accepted}
      end,
      now_fun: fn -> Agent.get(clock, & &1) end,
      inactivity_ms: @inactivity,
      tick_ms: 60_000,
      # Most unit tests start with control authority. Ownership-specific tests
      # override this; configured monitors start passive.
      ownership: :active,
      # Disable the recovery cooldown by default so lifecycle tests can drive
      # back-to-back recoveries on a frozen clock; the throttle has its own test.
      min_interval_ms: 0
    ]

    start_supervised!({ProducerMonitor, Keyword.merge(defaults, overrides)})
  end

  defp api_description(%Producer{} = producer) do
    %{
      id: producer.id,
      name: producer.name,
      active: true,
      api_url: "https://api.example.com/v1/#{producer.product}/",
      stateful_recovery_window_in_minutes: producer.recovery_window_minutes
    }
  end

  defp set_clock(clock, value), do: Agent.update(clock, fn _ -> value end)

  defp stored_snapshot do
    case ETS.load([]) do
      nil -> %Snapshot{}
      binary -> Snapshot.decode!(binary)
    end
  end

  defp store_snapshot(snapshot), do: ETS.save(Snapshot.encode(snapshot), [])

  defp update_stored_progress(id, fun) do
    snapshot = stored_snapshot()
    store_snapshot(Snapshot.put_progress(snapshot, id, fun.(Snapshot.progress(snapshot, id))))
  end

  defp put_checkpoint(id, timestamp) do
    update_stored_progress(id, &%{&1 | checkpoint: max(&1.checkpoint || timestamp, timestamp)})
  end

  defp checkpoint(id) do
    case Snapshot.progress(stored_snapshot(), id).checkpoint do
      nil -> :none
      timestamp -> {:ok, timestamp}
    end
  end

  defp save_state(checkpoints, synchronized_producers, consumer_tag \\ nil) do
    if consumer_tag do
      store_snapshot(Snapshot.change_session(stored_snapshot(), consumer_tag))
    end

    ## Health monitoring ---------------------------------------------------------
    Enum.each(checkpoints, fn {id, timestamp} -> put_checkpoint(id, timestamp) end)
    Enum.each(synchronized_producers, fn id -> update_stored_progress(id, &%{&1 | synchronized: true}) end)
  end

  defp resumable?(id), do: Snapshot.resumable?(stored_snapshot(), id)

  defp synchronized?(id), do: Snapshot.progress(stored_snapshot(), id).synchronized

  defp tick(monitor) do
    send(monitor, :tick)
    GenServer.call(monitor, :sync)
  end

  defp sync(monitor), do: GenServer.call(monitor, :sync)

  defp assert_recovery_triggered(product \\ "pre") do
    assert_receive {:recover_called, ^product, opts}
    opts[:request_id]
  end

  test "loads active producer descriptions when no producer list is injected" do
    descriptions = [
      %{
        id: 1,
        name: "Live Odds",
        active: true,
        api_url: "https://api.example.com/v1/liveodds/",
        stateful_recovery_window_in_minutes: 4320
      },
      %{
        id: 3,
        name: "Premium Cricket",
        active: false,
        api_url: "https://api.example.com/v1/premium_cricket",
        stateful_recovery_window_in_minutes: 4320
      }
    ]

    assert {:ok, %ProducerMonitor{producers: producers}} =
             ProducerMonitor.init(
               producer_fetcher: fn -> {:ok, %{producer: descriptions}} end,
               tick_ms: 60_000
             )

    assert %{
             1 => %Producer{
               name: "Live Odds",
               product: "liveodds",
               recovery_window_minutes: 4320
             }
           } = producers
  end

  test "fails startup when producer descriptions cannot be loaded" do
    assert_raise RuntimeError,
                 "could not load UOF producers: :timeout; no stored or configured producer descriptions",
                 fn ->
                   ProducerMonitor.init(
                     producer_fetcher: fn -> {:error, :timeout} end,
                     tick_ms: 60_000
                   )
                 end
  end

  test "uses an explicit runtime state struct", %{clock: clock} do
    monitor = start_monitor(clock)

    assert %ProducerMonitor{
             ownership: :active,
             producers: %{1 => %Producer{}},
             snapshot: %Snapshot{}
           } = :sys.get_state(monitor)
  end

  test "first alive triggers recovery; snapshot_complete brings producer up", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    sync(m)
    assert {:ok, %ProducerStatus{status: :recovering}} = ProducerMonitor.producer(m, 1)

    # :recovering is projected from the producer's canonical job, not
    # duplicated in its stored health lifecycle.
    assert %ProducerMonitor{producers: %{1 => %Producer{status: :down, recovery: %{job: %{}}}}} =
             :sys.get_state(m)

    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %ProducerStatus{id: 1, status: :up}}
  end

  test "checkpoints subscribed alive timestamps only after producer is up", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    sync(m)
    assert checkpoint(1) == :none

    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %ProducerStatus{status: :up}}

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
    assert_receive {:status, %ProducerStatus{status: :up}}

    # advance past inactivity with no new alive
    set_clock(clock, 1_000 + @inactivity + 1)
    tick(m)

    # Recovery is recorded at once but issued only when alives resume.
    assert_receive {:status, %ProducerStatus{status: :down}}
    refute_received {:recover_called, _product, _opts}

    flush_statuses()
    ProducerMonitor.alive(m, 1, 2_000, true)
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    rid2 = assert_recovery_triggered()

    # A later recovery follows the same status lifecycle.
    ProducerMonitor.snapshot_complete(m, 1, rid2)
    assert_receive {:status, %ProducerStatus{status: :up}}
  end

  describe "heartbeat timeout recovery waits for alives to resume" do
    test "records recovery durably before any request is issued", %{clock: clock} do
      m = start_monitor(clock)
      time_out_up_producer(m, clock)

      refute synchronized?(1)
      refute_received {:recover_called, _product, _opts}
      assert {:ok, %ProducerStatus{status: :down}} = ProducerMonitor.producer(m, 1)
      assert {:error, :already_recovering} = ProducerMonitor.recover(m, 1, [])
    end

    test "an unsubscribed alive releases a single request", %{clock: clock} do
      m = start_monitor(clock)
      time_out_up_producer(m, clock)

      ProducerMonitor.alive(m, 1, 2_000, false)
      assert_receive {:status, %ProducerStatus{status: :recovering}}
      assert_recovery_triggered()

      ProducerMonitor.alive(m, 1, 3_000, false)
      sync(m)
      refute_received {:recover_called, _product, _opts}
      refute_received {:status, _producer}
    end

    test "a reconnect while waiting does not issue the request", %{clock: clock} do
      m = start_monitor(clock)

      ProducerMonitor.observe_consumer_tag(m, :system_a)
      rid = assert_recovery_triggered()
      ProducerMonitor.snapshot_complete(m, 1, rid)
      assert_receive {:status, %ProducerStatus{status: :up}}

      set_clock(clock, 1_000 + @inactivity + 1)
      tick(m)
      assert_receive {:status, %ProducerStatus{status: :down}}
      flush_statuses()

      ProducerMonitor.observe_consumer_tag(m, :system_b)
      sync(m)
      refute_received {:recover_called, _product, _opts}
      refute_received {:status, _producer}

      ProducerMonitor.alive(m, 1, 2_000, true)
      assert_receive {:status, %ProducerStatus{status: :recovering}}
      assert_recovery_triggered()
    end

    test "clamps :after to the recovery window at the time of issue", %{clock: clock} do
      m = start_monitor([producers: [%Producer{id: 1, product: "pre", recovery_window_minutes: 60}]], clock)

      ProducerMonitor.alive(m, 1, 1_000, true)
      rid = assert_recovery_triggered()
      ProducerMonitor.snapshot_complete(m, 1, rid)
      assert_receive {:status, %ProducerStatus{status: :up}}
      ProducerMonitor.alive(m, 1, 5_000, true)
      sync(m)
      assert checkpoint(1) == {:ok, 5_000}

      set_clock(clock, 1_000 + @inactivity + 1)
      tick(m)
      assert_receive {:status, %ProducerStatus{status: :down}}

      # Alives resume after the checkpoint has left the 60 min window.
      now = 3 * 60 * 60_000
      set_clock(clock, now)
      ProducerMonitor.alive(m, 1, now, true)

      assert_receive {:recover_called, "pre", opts}
      assert opts[:after] == now - 60 * 60_000
    end
  end

  # Brings producer 1 up, then lets its heartbeat lapse.
  defp time_out_up_producer(m, clock) do
    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %ProducerStatus{status: :up}}

    set_clock(clock, 1_000 + @inactivity + 1)
    tick(m)
    assert_receive {:status, %ProducerStatus{status: :down}}
    flush_statuses()
  end

  # Drops status notifications already received, so a later assertion cannot
  # match one left over from an earlier transition.
  defp flush_statuses do
    receive do
      {:status, _producer} -> flush_statuses()
    after
      0 -> :ok
    end
  end

  defp bring_up(m) do
    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %ProducerStatus{status: :up}}
  end

  defp wait_until(fun, attempts \\ 100) do
    cond do
      fun.() -> :ok
      attempts == 0 -> flunk("condition not met")
      true -> Process.sleep(5) && wait_until(fun, attempts - 1)
    end
  end

  test "an up producer stays up however long content takes to process", %{clock: clock} do
    m = start_monitor(clock)
    bring_up(m)
    flush_statuses()

    # Content lag is not tracked: only heartbeats decide health.
    set_clock(clock, 1_000 + 5 * @inactivity)
    ProducerMonitor.alive(m, 1, 1_000 + 5 * @inactivity, true)
    tick(m)

    refute_received {:status, _producer}
    refute_received {:recover_called, _, _}
    assert {:ok, %ProducerStatus{status: :up}} = ProducerMonitor.producer(m, 1)
  end

  test "the first consumer tag recovers every producer, and a new tag recovers again", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.observe_consumer_tag(m, :conn_a)
    rid1 = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid1)
    assert_receive {:status, %ProducerStatus{status: :up}}

    # same consumer tag -> deduped, no recovery
    ProducerMonitor.observe_consumer_tag(m, :conn_a)
    sync(m)
    refute_received {:recover_called, _, _}

    # new consumer tag (a reconnect) -> recover
    ProducerMonitor.observe_consumer_tag(m, :conn_b)
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    assert_receive {:recover_called, "pre", _}
  end

  test "after a restart, an alive records recovery but defers HTTP until the consumer tag is observed", %{
    clock: clock
  } do
    save_state(%{}, [], "ctag-sys")
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    sync(m)

    refute_received {:recover_called, _, _}
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    assert {:ok, %ProducerStatus{status: :recovering}} = ProducerMonitor.producer(m, 1)

    ProducerMonitor.observe_consumer_tag(m, "ctag-sys")
    assert_receive {:recover_called, "pre", _}
  end

  test "pending startup recovery remains deferred before the readiness deadline", %{
    clock: clock
  } do
    save_state(%{}, [], "ctag-sys")
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    sync(m)
    assert_receive {:status, %ProducerStatus{status: :recovering}}

    set_clock(clock, 1_000 + @inactivity - 1)
    tick(m)

    refute_received {:recover_called, _, _}
    assert {:ok, %ProducerStatus{status: :recovering}} = ProducerMonitor.producer(m, 1)

    ProducerMonitor.observe_consumer_tag(m, "ctag-sys")
    assert_receive {:recover_called, "pre", _}
  end

  test "a consumer tag change replaces an in-flight recovery", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    first_request_id = assert_recovery_triggered()

    ProducerMonitor.observe_consumer_tag(m, :system_a)
    second_request_id = assert_recovery_triggered()
    assert second_request_id != first_request_id

    # Completion of the recovery that straddled the reconnect is stale.
    ProducerMonitor.snapshot_complete(m, 1, first_request_id)
    sync(m)
    assert {:ok, %ProducerStatus{status: :recovering}} = ProducerMonitor.producer(m, 1)

    ProducerMonitor.snapshot_complete(m, 1, second_request_id)
    assert_receive {:status, %ProducerStatus{status: :up}}
  end

  test "producers/1 returns all producers ordered by id", %{clock: clock} do
    m =
      start_monitor(
        [producers: [%Producer{id: 3, product: "pre"}, %Producer{id: 1, product: "liveodds"}]],
        clock
      )

    assert [%ProducerStatus{id: 1}, %ProducerStatus{id: 3}] = ProducerMonitor.producers(m)
  end

  test "producer/2 returns :error for unknown id", %{clock: clock} do
    m = start_monitor(clock)
    assert :error = ProducerMonitor.producer(m, 99)
  end

  test "full recovery (no checkpoint) omits :after", %{clock: clock} do
    m = start_monitor(clock)
    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_receive {:recover_called, "pre", opts}
    refute Keyword.has_key?(opts, :after)
  end

  test "incremental recovery uses the checkpoint as :after", %{clock: clock} do
    put_checkpoint(1, @now - 60_000)
    ## Recovery orchestration ----------------------------------------------------
    m = start_monitor([now_fun: fn -> @now end], clock)
    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_receive {:recover_called, "pre", opts}
    assert opts[:after] == @now - 60_000
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

  test "a new trigger within the min interval is deferred, not dropped", %{clock: clock} do
    # A small interval keeps the deferred-reissue timer fast; the recover_fun
    # counts issued requests so we can prove the second one is throttled.
    test_pid = self()
    calls = start_supervised!(%{id: :calls, start: {Agent, :start_link, [fn -> 0 end]}})

    recover_fun = fn product, opts ->
      Agent.update(calls, &(&1 + 1))
      send(test_pid, {:recover_called, product, opts})
      {:ok, :accepted}
    end

    m = start_monitor([recover_fun: recover_fun, min_interval_ms: 50], clock)

    # first recovery for this producer is never throttled
    ProducerMonitor.alive(m, 1, 1_000, true)
    rid1 = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid1)
    assert_receive {:status, %ProducerStatus{status: :up}}

    # a fresh trigger inside the cooldown is deferred: the producer transitions
    # to :recovering, but no request is issued yet
    assert :ok = ProducerMonitor.recover(m, 1, full: true)
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    refute_received {:recover_called, _, _}
    assert Agent.get(calls, & &1) == 1

    # it is not dropped: once the cooldown elapses the reissue fires
    assert_receive {:recover_called, "pre", _}, 500
    assert Agent.get(calls, & &1) == 2
  end

  test "triggers arriving during the cooldown do not stack deferred recoveries", %{clock: clock} do
    test_pid = self()
    calls = start_supervised!(%{id: :calls, start: {Agent, :start_link, [fn -> 0 end]}})

    recover_fun = fn product, opts ->
      Agent.update(calls, &(&1 + 1))
      send(test_pid, {:recover_called, product, opts})
      {:ok, :accepted}
    end

    # a generous interval so the synchronous re-triggers below cannot race the
    # deferred-reissue timer
    m = start_monitor([recover_fun: recover_fun, min_interval_ms: 300], clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid1 = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid1)
    assert_receive {:status, %ProducerStatus{status: :up}}

    # trigger inside the cooldown -> deferred (producer :recovering, no request)
    assert :ok = ProducerMonitor.recover(m, 1, full: true)
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    refute_received {:recover_called, _, _}

    # a flurry of further triggers while deferred are all dropped by the
    # :recovering guard, so no second defer is scheduled
    assert {:error, :already_recovering} = ProducerMonitor.recover(m, 1, [])
    ProducerMonitor.alive(m, 1, 1_000, true)
    ProducerMonitor.observe_consumer_tag(m, :conn_a)
    sync(m)
    refute_received {:recover_called, _, _}

    # exactly one reissue fires when the cooldown elapses
    assert_receive {:recover_called, "pre", _}, 1_000
    sync(m)
    assert Agent.get(calls, & &1) == 2
  end

  test "recovery cooldown is unaffected by a backward wall-clock adjustment", %{clock: clock} do
    m = start_monitor([min_interval_ms: 50], clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    first_request_id = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, first_request_id)
    assert_receive {:status, %ProducerStatus{status: :up}}

    set_clock(clock, -1_000_000)
    assert :ok = ProducerMonitor.recover(m, 1, full: true)

    # The wall-clock jump must not be added to the 50ms cooldown.
    assert_receive {:recover_called, "pre", _}, 500
  end

  test "non-matching snapshot_complete is ignored", %{clock: clock} do
    m = start_monitor(clock)
    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_receive {:recover_called, "pre", _}
    assert_receive {:status, %ProducerStatus{status: :recovering}}

    ProducerMonitor.snapshot_complete(m, 1, 9_999)
    sync(m)
    refute_received {:status, _}
    assert {:ok, %ProducerStatus{status: :recovering}} = ProducerMonitor.producer(m, 1)
  end

  test "manual recover goes through the full lifecycle", %{clock: clock} do
    put_checkpoint(1, @now - 60_000)
    m = start_monitor([now_fun: fn -> @now end], clock)

    assert :ok = ProducerMonitor.recover(m, 1, [])
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    assert_receive {:recover_called, "pre", opts}
    assert opts[:after] == @now - 60_000

    # a second trigger while one is in flight is refused
    assert {:error, :already_recovering} = ProducerMonitor.recover(m, 1, [])

    # and the completion correlates like any automatic recovery
    ProducerMonitor.snapshot_complete(m, 1, opts[:request_id])
    assert_receive {:status, %ProducerStatus{status: :up}}
  end

  test "manual full recover ignores the checkpoint", %{clock: clock} do
    put_checkpoint(1, @now - 60_000)
    m = start_monitor([now_fun: fn -> @now end], clock)

    assert :ok = ProducerMonitor.recover(m, 1, full: true)
    assert_receive {:recover_called, "pre", opts}
    refute Keyword.has_key?(opts, :after)

    assert {:error, :unknown_producer} = ProducerMonitor.recover(m, 99, full: true)
  end

  test "manual recovery is persisted but waits for the restored session", %{clock: clock} do
    save_state(%{1 => 1_000}, [1], "s1")
    m = start_monitor(clock)

    assert :ok = ProducerMonitor.recover(m, 1, [])
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    refute_received {:recover_called, _, _}
    refute resumable?(1)

    ProducerMonitor.observe_consumer_tag(m, "s1")
    assert_receive {:recover_called, "pre", opts}
    assert opts[:after] == 1_000
  end

  test "manual recovery completion establishes the alive timeout anchor", %{clock: clock} do
    m = start_monitor(clock)

    assert :ok = ProducerMonitor.recover(m, 1, full: true)
    assert_receive {:recover_called, "pre", opts}

    ProducerMonitor.snapshot_complete(m, 1, opts[:request_id])
    assert_receive {:status, %ProducerStatus{status: :up, last_alive_at: 1_000}}

    set_clock(clock, 1_000 + @inactivity + 1)
    tick(m)
    assert_receive {:status, %ProducerStatus{status: :down}}
    refute_received {:recover_called, _product, _opts}

    flush_statuses()
    ProducerMonitor.alive(m, 1, 2_000, true)
    assert_receive {:status, %ProducerStatus{status: :recovering}}
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

  test "an HTTP error from the UOF API schedules a retry", %{clock: clock} do
    test_pid = self()
    attempts = start_supervised!(%{id: :attempts, start: {Agent, :start_link, [fn -> 0 end]}})

    throttled = fn product, opts ->
      n = Agent.get_and_update(attempts, fn n -> {n, n + 1} end)
      send(test_pid, {:recover_called, product, opts})

      if n == 0 do
        {:error, %UOF.API.Error{type: :http, status: 429, message: "UOF API returned HTTP 429"}}
      else
        {:ok, nil}
      end
    end

    m = start_monitor([recover_fun: throttled, min_interval_ms: 10], clock)
    ProducerMonitor.alive(m, 1, 1_000, true)

    assert_receive {:recover_called, "pre", first}, 200
    assert_receive {:recover_called, "pre", second}, 500
    assert second[:request_id] != first[:request_id]

    ProducerMonitor.snapshot_complete(m, 1, second[:request_id])
    assert_receive {:status, %ProducerStatus{id: 1, status: :up}}
  end

  describe "recovery requests run outside the monitor" do
    # Each request reports itself and waits for the test to answer it.
    defp held_recover_fun(test_pid) do
      fn product, opts ->
        send(test_pid, {:held_request, product, opts, self()})

        receive do
          {:respond, result} -> result
        end
      end
    end

    test "the monitor keeps answering while a request is pending", %{clock: clock} do
      m = start_monitor([recover_fun: held_recover_fun(self())], clock)

      ProducerMonitor.alive(m, 1, 1_000, true)
      assert_receive {:held_request, "pre", _opts, task}

      assert {:ok, %ProducerStatus{status: :recovering}} = ProducerMonitor.producer(m, 1)

      send(task, {:respond, {:ok, :accepted}})
      sync(m)
    end

    test "a response arriving after the recovery completed is ignored", %{clock: clock} do
      m = start_monitor([recover_fun: held_recover_fun(self()), max_recovery_ms: 20], clock)

      ProducerMonitor.alive(m, 1, 1_000, true)
      assert_receive {:held_request, "pre", opts, task}

      ProducerMonitor.snapshot_complete(m, 1, opts[:request_id])
      assert_receive {:status, %ProducerStatus{status: :up}}

      # Neither a stall timer nor a retry follows the late response.
      send(task, {:respond, {:ok, :accepted}})
      refute_receive {:held_request, _product, _opts, _task}, 100
      assert {:ok, %ProducerStatus{status: :up}} = ProducerMonitor.producer(m, 1)
    end

    test "a failed response for a parked recovery is not retried", %{clock: clock} do
      m = start_monitor([recover_fun: held_recover_fun(self()), min_interval_ms: 20], clock)

      ProducerMonitor.alive(m, 1, 1_000, true)
      assert_receive {:held_request, "pre", _opts, task}

      report_active_state(m, :passive)
      send(task, {:respond, {:error, :boom}})
      refute_receive {:held_request, _product, _opts, _task}, 100

      # Promotion reissues the parked recovery once.
      report_active_state(m, :active)
      assert_receive {:held_request, "pre", _opts, _task}
    end
  end

  test "an exception from the recovery request crashes the monitor", %{clock: clock} do
    broken = fn _product, _opts -> raise ArgumentError, "bad recovery request" end

    m = start_monitor([recover_fun: broken], clock)
    ref = Process.monitor(m)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        ProducerMonitor.alive(m, 1, 1_000, true)
        assert_receive {:DOWN, ^ref, :process, ^m, {%ArgumentError{message: "bad recovery request"}, _}}
      end)

    assert log =~ "bad recovery request"
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
    assert_receive {:status, %ProducerStatus{id: 1, status: :up}}
  end

  describe "telemetry" do
    setup do
      attach_telemetry([
        [:uof_sdk, :recovery, :completed],
        [:uof_sdk, :recovery, :failed],
        [:uof_sdk, :recovery, :stalled],
        [:uof_sdk, :producer, :status]
      ])
    end

    test "a completed recovery reports its duration since the request", %{clock: clock} do
      monotonic = start_supervised!(%{id: :monotonic, start: {Agent, :start_link, [fn -> 100 end]}})
      m = start_monitor([monotonic_fun: fn -> Agent.get(monotonic, & &1) end], clock)

      ProducerMonitor.alive(m, 1, 1_000, true)
      rid = assert_recovery_triggered()
      Agent.update(monotonic, fn _ -> 350 end)
      ProducerMonitor.snapshot_complete(m, 1, rid)

      assert_receive {:telemetry, [:uof_sdk, :recovery, :completed], %{duration_ms: 250},
                      %{producer_id: 1, product: "pre", request_id: ^rid}}
    end

    test "a failed request reports its reason and retry delay", %{clock: clock} do
      m = start_monitor([recover_fun: fn _product, _opts -> {:error, :boom} end, min_interval_ms: 60_000], clock)

      ProducerMonitor.alive(m, 1, 1_000, true)

      assert_receive {:telemetry, [:uof_sdk, :recovery, :failed], _measurements,
                      %{producer_id: 1, reason: ":boom", retry_in_ms: 60_000, request_id: rid}}

      assert is_integer(rid)
    end

    test "a stalled request is reported before it is reissued", %{clock: clock} do
      m = start_monitor([max_recovery_ms: 20], clock)

      ProducerMonitor.alive(m, 1, 1_000, true)
      rid = assert_recovery_triggered()

      assert_receive {:telemetry, [:uof_sdk, :recovery, :stalled], _measurements, %{producer_id: 1, request_id: ^rid}},
                     500
    end

    test "status transitions are reported without a handler", %{clock: clock} do
      m = start_monitor([handler: nil], clock)

      ProducerMonitor.alive(m, 1, 1_000, true)
      assert_receive {:telemetry, [:uof_sdk, :producer, :status], _measurements, %{producer_id: 1, status: :recovering}}

      ProducerMonitor.snapshot_complete(m, 1, assert_recovery_triggered())
      assert_receive {:telemetry, [:uof_sdk, :producer, :status], _measurements, %{producer_id: 1, status: :up}}
    end
  end

  defp attach_telemetry(events) do
    test_pid = self()
    id = make_ref()

    :telemetry.attach_many(
      id,
      events,
      fn event, measurements, metadata, _config -> send(test_pid, {:telemetry, event, measurements, metadata}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
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

  describe "interrupted recoveries" do
    setup do
      attach_telemetry([[:uof_sdk, :recovery, :interrupted], [:uof_sdk, :recovery, :completed]])
    end

    test "an unsubscribed alive repeats the recovery from the last subscribed alive", %{clock: clock} do
      m = start_monitor(clock)

      ProducerMonitor.alive(m, 1, 1_000, true)
      rid1 = assert_recovery_triggered()
      assert_receive {:status, %ProducerStatus{status: :recovering}}

      ProducerMonitor.alive(m, 1, 2_000, true)
      ProducerMonitor.alive(m, 1, 3_000, false)
      # After the interruption, subscribed alives no longer prove continuity.
      ProducerMonitor.alive(m, 1, 4_000, true)
      sync(m)

      assert_receive {:telemetry, [:uof_sdk, :recovery, :interrupted], _, %{request_id: ^rid1, reason: :unsubscribed}}

      ProducerMonitor.snapshot_complete(m, 1, rid1)
      assert_receive {:recover_called, "pre", opts}
      assert opts[:after] == 2_000
      refute_received {:status, _producer}
      refute_received {:telemetry, [:uof_sdk, :recovery, :completed], _, _}
      assert {:ok, %ProducerStatus{status: :recovering}} = ProducerMonitor.producer(m, 1)

      ProducerMonitor.snapshot_complete(m, 1, opts[:request_id])
      assert_receive {:status, %ProducerStatus{status: :up}}
    end

    test "a heartbeat gap repeats the recovery from its original timestamp", %{clock: clock} do
      put_checkpoint(1, 500)
      m = start_monitor(clock)

      ProducerMonitor.alive(m, 1, 1_000, true)
      assert_receive {:recover_called, "pre", first}
      assert first[:after] == 500

      set_clock(clock, 1_000 + @inactivity + 1)
      tick(m)
      tick(m)

      assert_receive {:telemetry, [:uof_sdk, :recovery, :interrupted], _, %{reason: :alive_timeout}}
      refute_received {:telemetry, [:uof_sdk, :recovery, :interrupted], _, _}
      assert {:ok, %ProducerStatus{status: :recovering}} = ProducerMonitor.producer(m, 1)

      ProducerMonitor.snapshot_complete(m, 1, first[:request_id])
      assert_receive {:recover_called, "pre", second}
      assert second[:after] == 500

      ProducerMonitor.snapshot_complete(m, 1, second[:request_id])
      assert_receive {:status, %ProducerStatus{status: :up}}
    end

    test "a repeated recovery is clamped to the recovery window", %{clock: clock} do
      m = start_monitor([producers: [%Producer{id: 1, product: "pre", recovery_window_minutes: 60}]], clock)

      ProducerMonitor.alive(m, 1, 1_000, true)
      rid = assert_recovery_triggered()
      ProducerMonitor.alive(m, 1, 2_000, true)
      ProducerMonitor.alive(m, 1, 3_000, false)

      now = 3 * 60 * 60_000
      set_clock(clock, now)
      ProducerMonitor.snapshot_complete(m, 1, rid)

      assert_receive {:recover_called, "pre", opts}
      assert opts[:after] == now - 60 * 60_000
    end

    test "a reissued request covers an earlier interruption", %{clock: clock} do
      m = start_monitor([now_fun: fn -> @now end, max_recovery_ms: 20], clock)

      ProducerMonitor.alive(m, 1, 1_000, true)
      first = assert_recovery_triggered()
      ProducerMonitor.alive(m, 1, 2_000, false)

      # The stall reissues the request from its original timestamp.
      assert_receive {:recover_called, "pre", second}, 500
      assert second[:request_id] != first

      ProducerMonitor.snapshot_complete(m, 1, second[:request_id])
      assert_receive {:status, %ProducerStatus{status: :up}}
    end

    test "an unsubscribed alive before the request is issued does not interrupt it", %{clock: clock} do
      save_state(%{}, [], "ctag-sys")
      m = start_monitor(clock)

      # Recovery waits for the session; the request is not in flight yet.
      ProducerMonitor.alive(m, 1, 1_000, true)
      ProducerMonitor.alive(m, 1, 2_000, false)
      ProducerMonitor.observe_consumer_tag(m, "ctag-sys")
      rid = assert_recovery_triggered()

      ProducerMonitor.snapshot_complete(m, 1, rid)
      assert_receive {:status, %ProducerStatus{status: :up}}
      refute_received {:telemetry, [:uof_sdk, :recovery, :interrupted], _, _}
    end
  end

  ## Restart resume -------------------------------------------------------------

  # Persist the state of a producer that was up, with a checkpoint and the
  # consumer tag — the shape a healthy shutdown leaves behind.
  defp persist_healthy_shutdown(checkpoint) do
    save_state(%{1 => checkpoint}, [1], "ctag-sys")
  end

  test "resumed producer skips startup recovery and returns up in the same session", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    set_clock(clock, 50_000)
    m = start_monitor(clock)

    # same consumer tag after restart -> same upstream consume session -> no recovery
    ProducerMonitor.observe_consumer_tag(m, "ctag-sys")
    # subscribed alive on the resuming producer -> no recovery either
    ProducerMonitor.alive(m, 1, 50_000, true)
    sync(m)
    refute_received {:recover_called, _, _}
    assert {:ok, %ProducerStatus{status: :resuming}} = ProducerMonitor.producer(m, 1)

    tick(m)
    assert_receive {:status, %ProducerStatus{status: :up}}
    refute_received {:recover_called, _, _}
  end

  test "resumed producer waits for an alive generated within the heartbeat interval", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    set_clock(clock, 50_000)
    m = start_monitor(clock)
    ProducerMonitor.observe_consumer_tag(m, "ctag-sys")

    # Draining retained backlog: alives keep the producer alive, but a session
    # change or unsubscribed alive may still be queued behind them.
    ProducerMonitor.alive(m, 1, 50_000 - @inactivity - 1, true)
    tick(m)
    refute_received {:status, %ProducerStatus{status: :up}}
    assert {:ok, %ProducerStatus{status: :resuming, alive_delay: delay}} = ProducerMonitor.producer(m, 1)
    assert delay == @inactivity + 1

    ProducerMonitor.alive(m, 1, 49_000, true)
    tick(m)
    assert_receive {:status, %ProducerStatus{status: :up, alive_delay: 1_000}}
    refute_received {:recover_called, _, _}
  end

  test "each alive reports how late it was read", %{clock: clock} do
    attach_telemetry([[:uof_sdk, :producer, :alive]])
    set_clock(clock, 5_000)
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 4_200, false)
    sync(m)

    assert_receive {:telemetry, [:uof_sdk, :producer, :alive], %{delay_ms: 800}, %{producer_id: 1, subscribed: false}}
  end

  test "resumed producer stays resuming until the consumer tag is observed", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    set_clock(clock, 1_500)
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_600, true)
    sync(m)
    refute_received {:recover_called, _, _}

    tick(m)
    refute_received {:status, %ProducerStatus{status: :up}}
    assert {:ok, %ProducerStatus{status: :resuming}} = ProducerMonitor.producer(m, 1)

    ProducerMonitor.observe_consumer_tag(m, "ctag-sys")
    tick(m)
    assert_receive {:status, %ProducerStatus{status: :up}}
  end

  test "resumed producer stays resuming without a current alive even when the consumer tag is observed", %{
    clock: clock
  } do
    persist_healthy_shutdown(1_000)
    set_clock(clock, 1_500)
    m = start_monitor(clock)

    ProducerMonitor.observe_consumer_tag(m, "ctag-sys")
    tick(m)

    refute_received {:status, %ProducerStatus{status: :up}}
    assert {:ok, %ProducerStatus{status: :resuming, last_alive_at: nil}} = ProducerMonitor.producer(m, 1)

    ProducerMonitor.alive(m, 1, 1_600, true)
    tick(m)
    assert_receive {:status, %ProducerStatus{status: :up, last_alive_at: 1_500}}
  end

  test "resumed producer without a current alive times out through the normal health check", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    m = start_monitor(clock)

    ProducerMonitor.observe_consumer_tag(m, "ctag-sys")
    sync(m)

    set_clock(clock, 1_000 + @inactivity + 1)
    tick(m)

    assert_receive {:status, %ProducerStatus{status: :down}}
    refute_received {:recover_called, _product, _opts}

    flush_statuses()
    ProducerMonitor.alive(m, 1, 2_000, true)
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    assert_receive {:recover_called, "pre", opts}
    assert opts[:after] == 1_000
  end

  test "session startup has a bounded readiness deadline", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    m = start_monitor(clock)
    state = :sys.get_state(m)

    set_clock(clock, 1_000 + @inactivity + 1)

    assert_raise RuntimeError,
                 "producer monitor session readiness timed out; no consumer tag observed",
                 fn -> ProducerMonitor.handle_info(:tick, state) end
  end

  test "resumed producer recovers incrementally when the consumer tag changed across restart", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    set_clock(clock, 50_000)
    m = start_monitor(clock)

    ProducerMonitor.observe_consumer_tag(m, "ctag-sys-new")

    assert_receive {:status, %ProducerStatus{status: :recovering}}
    assert_receive {:recover_called, "pre", opts}
    assert opts[:after] == 1_000
  end

  test "a session change saved before a restart leaves producers unsynchronized", %{clock: clock} do
    persist_healthy_shutdown(1_000)

    # Simulate a crash after the session change is saved but before the
    # monitor visits producer 1 to prepare its recovery.
    store_snapshot(Snapshot.change_session(stored_snapshot(), "ctag-sys"))

    m = start_monitor(clock)
    assert {:ok, %ProducerStatus{status: :down}} = ProducerMonitor.producer(m, 1)

    ProducerMonitor.alive(m, 1, 1_000, true)
    sync(m)
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    refute_received {:recover_called, _, _}

    ProducerMonitor.observe_consumer_tag(m, "ctag-sys")

    assert_receive {:recover_called, "pre", opts}
    assert opts[:after] == 1_000
  end

  test "recovery intent is saved before the recovery request", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    set_clock(clock, 50_000)
    m = start_monitor([monitor_store: {RecordingStore, pid: self()}], clock)
    # Startup stores the loaded descriptions.
    assert_receive {:saved, %Snapshot{descriptions: [%{id: 1}]}}

    ProducerMonitor.observe_consumer_tag(m, "ctag-sys")
    ProducerMonitor.alive(m, 1, 50_000, false)

    # The monitor sends both messages, so mailbox order is issue order.
    assert [{:saved, saved}, {:recover_called, "pre", _opts}] = saves_and_requests([])
    refute Snapshot.resumable?(saved, 1)
  end

  defp saves_and_requests(acc) do
    receive do
      {:saved, _snapshot} = saved -> saves_and_requests([saved | acc])
      {:recover_called, _product, _opts} = request -> Enum.reverse([request | acc])
    after
      1_000 -> flunk("no recovery request after #{inspect(Enum.reverse(acc))}")
    end
  end

  test "a failing save crashes the monitor before the recovery request", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    # Matching stored descriptions leave startup nothing to save.
    store_snapshot(%{
      stored_snapshot()
      | descriptions: [%{id: 1, name: nil, product: "pre", recovery_window_minutes: nil}]
    })

    set_clock(clock, 50_000)
    m = start_monitor([monitor_store: {FailingStore, []}], clock)
    ref = Process.monitor(m)

    ProducerMonitor.observe_consumer_tag(m, "ctag-sys")
    ProducerMonitor.alive(m, 1, 50_000, false)

    assert_receive {:DOWN, ^ref, :process, ^m, {%RuntimeError{message: "store unavailable"}, _}}
    refute_received {:recover_called, _, _}
    assert resumable?(1)
  end

  test "an unreadable snapshot fails startup instead of loading as empty" do
    ETS.save("garbage", [])

    assert_raise ArgumentError, ~r/unreadable producer monitor snapshot/, fn ->
      ProducerMonitor.init(producers: [%Producer{id: 1, product: "pre"}], tick_ms: 60_000)
    end
  end

  describe "producer descriptions" do
    setup do
      attach_telemetry([[:uof_sdk, :producers, :loaded]])
    end

    test "API descriptions are stored for later startups", %{clock: clock} do
      start_monitor(clock)

      assert_receive {:telemetry, [:uof_sdk, :producers, :loaded], %{count: 1}, %{source: :api}}
      assert [%{id: 1, product: "pre"}] = stored_snapshot().descriptions
    end

    test "starts from stored descriptions when the API fails and reloads in the background", %{clock: clock} do
      store_snapshot(%Snapshot{descriptions: [%{id: 1, name: nil, product: "pre", recovery_window_minutes: nil}]})
      calls = start_supervised!(%{id: :calls, start: {Agent, :start_link, [fn -> 0 end]}})

      fetcher = fn ->
        case Agent.get_and_update(calls, &{&1, &1 + 1}) do
          0 ->
            {:error, :unavailable}

          _ ->
            {:ok,
             %{
               producer:
                 Enum.map([%Producer{id: 1, product: "pre"}, %Producer{id: 2, product: "live"}], &api_description/1)
             }}
        end
      end

      m = start_monitor([producer_fetcher: fetcher, descriptions_refresh_ms: 20], clock)

      assert_receive {:telemetry, [:uof_sdk, :producers, :loaded], %{count: 1}, %{source: :snapshot}}
      assert {:ok, %ProducerStatus{id: 1}} = ProducerMonitor.producer(m, 1)

      assert_receive {:telemetry, [:uof_sdk, :producers, :loaded], %{count: 2}, %{source: :api}}, 1_000
      assert {:ok, %ProducerStatus{id: 2, status: :down}} = ProducerMonitor.producer(m, 2)
      assert [%{id: 1}, %{id: 2}] = stored_snapshot().descriptions
    end

    test "starts from configured producers when the API fails and nothing is stored" do
      configured = [%{id: 7, name: "Live Odds", product: "liveodds", recovery_window_minutes: 4320}]

      assert {:ok, %ProducerMonitor{producers: %{7 => %Producer{product: "liveodds", recovery_window_minutes: 4320}}}} =
               ProducerMonitor.init(
                 producer_fetcher: fn -> {:error, :unavailable} end,
                 producers: configured,
                 tick_ms: 60_000
               )

      assert_receive {:telemetry, [:uof_sdk, :producers, :loaded], %{count: 1}, %{source: :config}}
    end

    test "stored descriptions take precedence over configured producers" do
      store_snapshot(%Snapshot{descriptions: [%{id: 1, name: nil, product: "pre", recovery_window_minutes: nil}]})

      assert {:ok, %ProducerMonitor{producers: producers}} =
               ProducerMonitor.init(
                 producer_fetcher: fn -> {:error, :unavailable} end,
                 producers: [%{id: 7, name: nil, product: "liveodds", recovery_window_minutes: nil}],
                 tick_ms: 60_000
               )

      assert Map.keys(producers) == [1]
    end

    test "the monitor keeps answering while a reload is pending", %{clock: clock} do
      test_pid = self()
      calls = start_supervised!(%{id: :calls, start: {Agent, :start_link, [fn -> 0 end]}})

      fetcher = fn ->
        if Agent.get_and_update(calls, &{&1, &1 + 1}) > 0 do
          send(test_pid, {:reloading, self()})

          receive do
            :respond -> :ok
          end
        end

        known = if Agent.get(calls, & &1) == 1, do: [1], else: [1, 2]
        {:ok, %{producer: Enum.map(known, &api_description(%Producer{id: &1, product: "p#{&1}"}))}}
      end

      m = start_monitor([producer_fetcher: fetcher], clock)

      ProducerMonitor.alive(m, 2, 1_000, true)
      assert_receive {:reloading, task}

      # The reload is in flight, yet the monitor answers and records heartbeats.
      assert :error = ProducerMonitor.producer(m, 2)
      ProducerMonitor.alive(m, 1, 1_000, true)
      assert_recovery_triggered("p1")

      send(task, :respond)
      wait_until(fn -> match?({:ok, _}, ProducerMonitor.producer(m, 2)) end)
    end

    test "an alive from an unknown producer reloads descriptions at most once per interval", %{clock: clock} do
      calls = start_supervised!(%{id: :calls, start: {Agent, :start_link, [fn -> 0 end]}})

      fetcher = fn ->
        known = if Agent.get_and_update(calls, &{&1, &1 + 1}) == 0, do: [1], else: [1, 2]
        {:ok, %{producer: Enum.map(known, &api_description(%Producer{id: &1, product: "p#{&1}"}))}}
      end

      m = start_monitor([producer_fetcher: fetcher], clock)

      # Producer 2 was activated after startup: its first alive is dropped
      # and reloads the descriptions; the next one starts its recovery.
      ProducerMonitor.alive(m, 2, 1_000, true)
      wait_until(fn -> match?({:ok, _}, ProducerMonitor.producer(m, 2)) end)

      ProducerMonitor.alive(m, 2, 2_000, true)
      assert_recovery_triggered("p2")

      # Another unknown producer within the interval waits for the next slot.
      ProducerMonitor.alive(m, 3, 3_000, true)
      sync(m)
      assert Agent.get(calls, & &1) == 2
    end
  end

  test "a missing snapshot starts every producer down", %{clock: clock} do
    m = start_monitor(clock)
    assert {:ok, %ProducerStatus{status: :down}} = ProducerMonitor.producer(m, 1)
    # Only the loaded descriptions are stored.
    assert %Snapshot{consumer_tag: nil, producers: progress, descriptions: [%{id: 1}]} = stored_snapshot()
    assert progress == %{}
  end

  test "resumed producer recovers on an unsubscribed alive", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    set_clock(clock, 50_000)
    m = start_monitor(clock)

    ProducerMonitor.observe_consumer_tag(m, "ctag-sys")
    ProducerMonitor.alive(m, 1, 50_000, false)
    assert_recovery_triggered()

    # Starting recovery must replace the resumable persisted state.
    sync(m)
    refute resumable?(1)
  end

  test "unsubscribed alive remains pending until the unchanged consumer tag is observed", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, false)
    sync(m)

    assert_receive {:status, %ProducerStatus{status: :recovering}}
    refute_received {:recover_called, _, _}
    refute resumable?(1)

    # The unchanged consumer tag does not itself indicate a reconnect. Becoming ready
    # must nevertheless issue the recovery remembered from subscribed=0.
    ProducerMonitor.observe_consumer_tag(m, "ctag-sys")
    assert_receive {:recover_called, "pre", opts}
    assert opts[:after] == 1_000

    tick(m)
    assert {:ok, %ProducerStatus{status: :recovering}} = ProducerMonitor.producer(m, 1)
  end

  test "does not resume a producer persisted as plain down", %{clock: clock} do
    save_state(%{1 => 1_000}, [])
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_recovery_triggered()
  end

  test "does not resume without a checkpoint", %{clock: clock} do
    save_state(%{}, [1])
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_recovery_triggered()
  end

  test "resumes a producer persisted as resumable", %{clock: clock} do
    save_state(%{1 => 1_000}, [1])
    set_clock(clock, 50_000)
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 50_000, true)
    sync(m)
    refute_received {:recover_called, _, _}
    assert {:ok, %ProducerStatus{status: :resuming}} = ProducerMonitor.producer(m, 1)
  end

  test "persists whether a producer is synchronized", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()

    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %ProducerStatus{status: :up}}
    assert synchronized?(1)

    # silence -> down + recovering; persisted flattened so a restart re-recovers
    set_clock(clock, 1_000 + @inactivity + 1)
    tick(m)
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    refute synchronized?(1)
  end

  test "persists the consumer tag as it changes", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.observe_consumer_tag(m, "ctag-a")
    sync(m)
    assert stored_snapshot().consumer_tag == "ctag-a"

    ProducerMonitor.observe_consumer_tag(m, "ctag-b")
    sync(m)
    assert stored_snapshot().consumer_tag == "ctag-b"
  end

  defp report_active_state(monitor, active_state) do
    ProducerMonitor.active_state_change(monitor, %{
      active_state: active_state,
      topic: "uof-feed",
      subscription: "uof-sdk-system",
      consumer_pid: self()
    })

    sync(monitor)
  end

  test "a monitor starts passive until ownership is reported", %{clock: clock} do
    m = start_monitor([ownership: :passive], clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    sync(m)
    refute_receive {:recover_called, _product, _opts}
    assert ProducerMonitor.recover(m, 1, []) == {:error, :passive}

    report_active_state(m, :active)
    ProducerMonitor.alive(m, 1, 2_000, true)
    assert_recovery_triggered()
  end

  test "passive suppresses alive-gap recovery; promotion recovers from checkpoint", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %ProducerStatus{status: :up}}

    ProducerMonitor.alive(m, 1, 2_000, true)
    sync(m)
    assert checkpoint(1) == {:ok, 2_000}

    report_active_state(m, :passive)
    set_clock(clock, 1_000 + @inactivity + 1)
    tick(m)

    refute_receive {:recover_called, _product, _opts}
    assert {:ok, %ProducerStatus{status: :up}} = ProducerMonitor.producer(m, 1)

    report_active_state(m, :active)
    tick(m)

    assert_receive {:status, %ProducerStatus{status: :down}}
    refute_received {:recover_called, _product, _opts}

    flush_statuses()
    ProducerMonitor.alive(m, 1, 3_000, true)
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    assert_receive {:recover_called, "pre", opts}
    assert opts[:after] == 2_000
  end

  test "demotion parks an in-flight recovery; promotion reissues it", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid1 = assert_recovery_triggered()

    report_active_state(m, :passive)

    # The completion for the parked request now belongs to the new owner and
    # must not bring this instance's producer up.
    ProducerMonitor.snapshot_complete(m, 1, rid1)
    sync(m)
    assert {:ok, %ProducerStatus{status: :recovering}} = ProducerMonitor.producer(m, 1)

    report_active_state(m, :active)
    rid2 = assert_recovery_triggered()
    assert rid2 != rid1

    ProducerMonitor.snapshot_complete(m, 1, rid2)
    assert_receive {:status, %ProducerStatus{status: :up}}
  end

  test "a retry timer firing while passive waits for promotion", %{clock: clock} do
    test_pid = self()
    attempts = start_supervised!(%{id: :ownership_retry_attempts, start: {Agent, :start_link, [fn -> 0 end]}})

    recover_fun = fn product, opts ->
      attempt = Agent.get_and_update(attempts, fn attempt -> {attempt, attempt + 1} end)
      send(test_pid, {:recover_called, product, opts})
      if attempt == 0, do: {:error, :boom}, else: {:ok, :accepted}
    end

    m = start_monitor([recover_fun: recover_fun, min_interval_ms: 50], clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_receive {:recover_called, "pre", _opts}
    report_active_state(m, :passive)

    ## Control-plane ownership ---------------------------------------------------

    # The API retry becomes due while passive, but only the active owner may
    # issue it.
    refute_receive {:recover_called, "pre", _opts}, 100

    report_active_state(m, :active)
    assert_receive {:recover_called, "pre", _opts}, 500
  end

  test "manual recovery is refused while passive", %{clock: clock} do
    m = start_monitor(clock)

    report_active_state(m, :passive)
    assert ProducerMonitor.recover(m, 1, []) == {:error, :passive}

    report_active_state(m, :active)
    assert ProducerMonitor.recover(m, 1, []) == :ok
    assert_recovery_triggered()
  end

  test "unknown producer takes precedence over passive ownership", %{clock: clock} do
    m = start_monitor([ownership: :passive], clock)

    assert ProducerMonitor.recover(m, 99, []) == {:error, :unknown_producer}
  end

  test "an alive racing a passive report preserves recovery until promotion", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    m = start_monitor(clock)

    report_active_state(m, :passive)
    ProducerMonitor.alive(m, 1, 1_000, false)
    sync(m)

    refute_receive {:recover_called, _product, _opts}
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    assert {:ok, %ProducerStatus{status: :recovering}} = ProducerMonitor.producer(m, 1)
    refute resumable?(1)

    report_active_state(m, :active)
    refute_received {:recover_called, _, _}

    ProducerMonitor.observe_consumer_tag(m, "ctag-sys")
    assert_recovery_triggered()
  end

  test "repeated active-state reports are idempotent", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_recovery_triggered()

    report_active_state(m, :passive)
    report_active_state(m, :passive)
    report_active_state(m, :active)
    rid = assert_recovery_triggered()

    # A repeat of the current state must not reissue the in-flight recovery.
    report_active_state(m, :active)
    refute_receive {:recover_called, _product, _opts}

    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %ProducerStatus{status: :up}}
  end

  defp establish_session(m) do
    ProducerMonitor.observe_consumer_tag(m, "s1")
    rid = assert_recovery_triggered()
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %ProducerStatus{status: :up}}
  end

  test "stale consumer tag observations after a reconnect do not re-trigger recovery", %{clock: clock} do
    m = start_monitor(clock)
    establish_session(m)

    # genuine reconnect: never-seen consumer tag
    ProducerMonitor.observe_consumer_tag(m, "s2")
    assert_recovery_triggered()

    # straggler from the superseded session, delivered out of order
    ProducerMonitor.observe_consumer_tag(m, "s1")
    sync(m)

    refute_receive {:recover_called, _product, _opts}
    assert stored_snapshot().consumer_tag == "s2"
  end

  test "a persisted consumer tag arms the restart session gate before observations arrive", %{clock: clock} do
    save_state(%{}, [], "old-s")
    m = start_monitor(clock)

    # The persisted baseline identifies a restart window. Recovery intent is
    # durable, but HTTP must wait until the system pipeline has reattached.
    ProducerMonitor.alive(m, 1, 1_000, true)
    sync(m)
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    refute_received {:recover_called, _, _}

    ProducerMonitor.observe_consumer_tag(m, "old-s")
    assert_recovery_triggered()
  end

  test "session observations are ignored while passive and re-detected on promotion", %{clock: clock} do
    m = start_monitor(clock)
    establish_session(m)
    report_active_state(m, :passive)

    ProducerMonitor.observe_consumer_tag(m, "s2")
    sync(m)

    # nothing recorded, nothing acted on: the owner heals the shared feed
    refute_receive {:recover_called, _product, _opts}
    refute_receive {:status, _producer}
    assert {:ok, %ProducerStatus{status: :up}} = ProducerMonitor.producer(m, 1)
    assert stored_snapshot().consumer_tag == "s1"
    assert synchronized?(1)

    # the stale baseline re-detects the session change once promoted
    report_active_state(m, :active)
    ProducerMonitor.observe_consumer_tag(m, "s2")
    assert_recovery_triggered()
    sync(m)
    assert stored_snapshot().consumer_tag == "s2"
  end

  test "a stale retry timer does not issue recovery for a healthy producer", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %ProducerStatus{status: :up}}

    send(m, {:retry, 1, nil})
    sync(m)

    refute_receive {:recover_called, _product, _opts}
    assert {:ok, %ProducerStatus{status: :up}} = ProducerMonitor.producer(m, 1)
  end

  test "a nil alive timestamp does not clobber the checkpoint", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %ProducerStatus{status: :up}}

    ProducerMonitor.alive(m, 1, 2_000, true)
    sync(m)
    assert checkpoint(1) == {:ok, 2_000}

    ProducerMonitor.alive(m, 1, nil, true)
    sync(m)
    assert checkpoint(1) == {:ok, 2_000}
  end

  ## Consume sessions ----------------------------------------------------------

  ## Timer and checkpoint hygiene ---------------------------------------------
end

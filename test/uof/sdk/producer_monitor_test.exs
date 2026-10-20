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
      recovery_overlap_ms: 0,
      # Most unit tests start with control authority. Ownership-specific tests
      # override this; configured Pulsar monitors start passive.
      ownership: {:failover, :active},
      # Disable the recovery cooldown by default so lifecycle tests can drive
      # back-to-back recoveries on a frozen clock; the throttle has its own test.
      min_interval_ms: 0
    ]

    start_supervised!({ProducerMonitor, Keyword.merge(defaults, overrides)})
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

  defp save_state(checkpoints, synchronized_producers, consumer_tags \\ %{}) do
    if map_size(consumer_tags) > 0 do
      store_snapshot(Snapshot.change_session(stored_snapshot(), consumer_tags))
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
    assert_raise RuntimeError, "could not load UOF producers: {:error, :timeout}", fn ->
      ProducerMonitor.init(
        producer_fetcher: fn -> {:error, :timeout} end,
        tick_ms: 60_000
      )
    end
  end

  test "uses an explicit runtime state struct", %{clock: clock} do
    monitor = start_monitor(clock)

    assert %ProducerMonitor{
             ownership: {:failover, :active},
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

    assert_receive {:status, %ProducerStatus{status: :recovering}}
    rid2 = assert_recovery_triggered()

    # A later recovery follows the same status lifecycle.
    ProducerMonitor.snapshot_complete(m, 1, rid2)
    assert_receive {:status, %ProducerStatus{status: :up}}
  end

  test "processing lag marks down + delayed without recovering", %{clock: clock} do
    m = start_monitor(clock)

    # bring it up first
    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %ProducerStatus{status: :up}}

    # Local clock jumps to 20_000, but the newest message the content pipeline
    # processed was generated back at 1_000. A fresh system alive keeps the
    # alive-interval path quiet, so the processing check is what trips.
    set_clock(clock, 20_000)
    ProducerMonitor.alive(m, 1, 20_000, true)
    ProducerMonitor.message(m, 1, 1_000)
    sync(m)

    tick(m)
    assert_receive {:status, %ProducerStatus{status: :delayed}}

    refute_received {:recover_called, _, _}

    # content progress generated at ~now arrives -> caught up -> stabilized
    ProducerMonitor.message(m, 1, 20_000)
    tick(m)

    assert_receive {:status, %ProducerStatus{status: :up}}
  end

  test "content-session alive freshness prevents quiet producers from being marked delayed", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %ProducerStatus{status: :up}}

    set_clock(clock, 20_000)

    # System alive keeps delivery health/checkpointing fresh. The content
    # session's own alive is queued behind content and therefore proves local
    # content processing is not lagging, even when there are no event messages.
    ProducerMonitor.alive(m, 1, 20_000, true)
    ProducerMonitor.message(m, 1, 20_000)
    tick(m)

    refute_received {:status, %ProducerStatus{status: :delayed}}
    refute_received {:recover_called, _, _}
    assert {:ok, %ProducerStatus{status: :up}} = ProducerMonitor.producer(m, 1)
  end

  test "content-session alive freshness does not write checkpoints", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %ProducerStatus{status: :up}}

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
    assert_receive {:status, %ProducerStatus{status: :up}}

    # drive it into processing lag: local clock at 20_000 but
    # content progress is still back at 1_000, with a fresh alive so the
    # alive-interval path stays quiet and the processing check is what trips.
    set_clock(clock, 20_000)
    ProducerMonitor.alive(m, 1, 20_000, true)
    ProducerMonitor.message(m, 1, 1_000)
    sync(m)
    tick(m)
    assert_receive {:status, %ProducerStatus{status: :delayed}}

    # another subscribed alive arrives while still delayed and still behind: the
    # remote feed is healthy, so this must NOT issue a recovery (the flop bug
    # re-triggered on every alive).
    ProducerMonitor.alive(m, 1, 1_000, true)
    sync(m)
    refute_received {:recover_called, _, _}
    assert {:ok, %ProducerStatus{status: :delayed}} = ProducerMonitor.producer(m, 1)
  end

  test "startup connection recovery waits for both system and content namespaces", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, :conn_a})
    sync(m)
    refute_received {:recover_called, _, _}

    ProducerMonitor.observe_connection(m, {:content, :conn_a})
    rid1 = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid1)
    assert_receive {:status, %ProducerStatus{status: :up}}

    # same consumer tag -> deduped, no recovery
    ProducerMonitor.observe_connection(m, {:system, :conn_a})
    sync(m)
    refute_received {:recover_called, _, _}

    # new consumer tag (a reconnect) -> down + recover
    ProducerMonitor.observe_connection(m, {:system, :conn_b})
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    assert_receive {:recover_called, "pre", _}
  end

  test "startup alive records recovery but defers HTTP until both connections are ready", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, :conn_a})
    ProducerMonitor.alive(m, 1, 1_000, true)
    sync(m)

    refute_received {:recover_called, _, _}
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    assert {:ok, %ProducerStatus{status: :recovering}} = ProducerMonitor.producer(m, 1)

    ProducerMonitor.observe_connection(m, {:content, :conn_a})
    assert_receive {:recover_called, "pre", _}
  end

  test "pending startup recovery remains deferred before the readiness deadline", %{
    clock: clock
  } do
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, :conn_a})
    ProducerMonitor.alive(m, 1, 1_000, true)
    sync(m)
    assert_receive {:status, %ProducerStatus{status: :recovering}}

    set_clock(clock, 1_000 + @inactivity - 1)
    tick(m)

    refute_received {:recover_called, _, _}
    assert {:ok, %ProducerStatus{status: :recovering}} = ProducerMonitor.producer(m, 1)

    ProducerMonitor.observe_connection(m, {:content, :conn_a})
    assert_receive {:recover_called, "pre", _}
  end

  test "first consumer tag from the second namespace triggers one startup recovery", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, "same-ctag"})
    sync(m)
    refute_received {:status, %ProducerStatus{status: :recovering}}
    refute_received {:recover_called, _, _}

    ProducerMonitor.observe_connection(m, {:content, "same-ctag"})
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    rid1 = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid1)
    assert_receive {:status, %ProducerStatus{status: :up}}
  end

  test "same namespace consumer tag changes before startup is ready do not recover early", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, :system_a})
    ProducerMonitor.observe_connection(m, {:system, :system_b})
    sync(m)
    refute_received {:recover_called, _, _}

    ProducerMonitor.observe_connection(m, {:content, :content_a})
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    assert_receive {:recover_called, "pre", _}
  end

  test "startup gate requires system and content namespaces explicitly", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:default, :default_a})
    ProducerMonitor.observe_connection(m, {:system, :system_a})
    sync(m)
    refute_received {:recover_called, _, _}

    ProducerMonitor.observe_connection(m, {:content, :content_a})
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    assert_receive {:recover_called, "pre", _}
  end

  test "consumer tag changes in either namespace trigger recovery after startup", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, :system_a})
    sync(m)
    refute_received {:recover_called, _, _}

    ProducerMonitor.observe_connection(m, {:content, :content_a})
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    rid1 = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid1)
    assert_receive {:status, %ProducerStatus{status: :up}}

    ProducerMonitor.observe_connection(m, {:content, :content_b})
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    rid2 = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid2)
    assert_receive {:status, %ProducerStatus{status: :up}}

    ProducerMonitor.observe_connection(m, {:system, :system_b})
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    assert_receive {:recover_called, "pre", _}
  end

  test "a consumer tag change replaces an in-flight recovery", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    first_request_id = assert_recovery_triggered()

    ProducerMonitor.observe_connection(m, {:system, :system_a})
    ProducerMonitor.observe_connection(m, {:content, :content_a})
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
    ProducerMonitor.observe_connection(m, {:system, :conn_a})
    ProducerMonitor.observe_connection(m, {:content, :conn_a})
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

  test "manual recovery is persisted but waits for restored connections", %{clock: clock} do
    save_state(%{1 => 1_000}, [1], %{system: "s1", content: "c1"})
    m = start_monitor(clock)

    assert :ok = ProducerMonitor.recover(m, 1, [])
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    refute_received {:recover_called, _, _}
    refute resumable?(1)

    ProducerMonitor.observe_connection(m, {:system, "s1"})
    sync(m)
    refute_received {:recover_called, _, _}

    ProducerMonitor.observe_connection(m, {:content, "c1"})
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

  ## Restart resume -------------------------------------------------------------

  # Persist the state of a producer that was up, with a checkpoint and both
  # namespace consumer tags — the shape a healthy shutdown leaves behind.
  defp persist_healthy_shutdown(checkpoint) do
    save_state(
      %{1 => checkpoint},
      [1],
      %{system: "ctag-sys", content: "ctag-con"}
    )
  end

  test "resumed producer skips startup recovery and returns up once caught up", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    set_clock(clock, 50_000)
    m = start_monitor(clock)

    # same consumer tags after restart -> same upstream consume session -> no recovery
    ProducerMonitor.observe_connection(m, {:system, "ctag-sys"})
    ProducerMonitor.observe_connection(m, {:content, "ctag-con"})
    # subscribed alive on the resuming producer -> no recovery either
    ProducerMonitor.alive(m, 1, 50_000, true)
    sync(m)
    refute_received {:recover_called, _, _}
    assert {:ok, %ProducerStatus{status: :resuming}} = ProducerMonitor.producer(m, 1)

    # still draining: last_message_timestamp is seeded from the checkpoint
    tick(m)
    refute_received {:status, %ProducerStatus{status: :up}}

    # backlog catches up -> stabilized -> up, without ever recovering
    ProducerMonitor.message(m, 1, 50_000)
    tick(m)
    assert_receive {:status, %ProducerStatus{status: :up}}
    refute_received {:recover_called, _, _}
  end

  test "resumed producer stays down until an alive and both connections are observed", %{clock: clock} do
    # checkpoint is recent, so the processing-lag check alone would not hold the
    # producer down — the heartbeat gate is what must keep it down here
    persist_healthy_shutdown(1_000)
    set_clock(clock, 1_500)
    m = start_monitor(clock)

    tick(m)
    refute_received {:status, %ProducerStatus{status: :up}}
    assert {:ok, %ProducerStatus{status: :resuming}} = ProducerMonitor.producer(m, 1)

    ProducerMonitor.observe_connection(m, {:system, "ctag-sys"})
    ProducerMonitor.alive(m, 1, 1_600, true)
    sync(m)
    refute_received {:recover_called, _, _}

    tick(m)
    refute_received {:status, %ProducerStatus{status: :up}}

    ProducerMonitor.observe_connection(m, {:content, "ctag-con"})
    tick(m)
    assert_receive {:status, %ProducerStatus{status: :up}}
  end

  test "resumed producer stays resuming without a current alive even when both connections are ready", %{
    clock: clock
  } do
    persist_healthy_shutdown(1_000)
    set_clock(clock, 1_500)
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, "ctag-sys"})
    ProducerMonitor.observe_connection(m, {:content, "ctag-con"})
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

    ProducerMonitor.observe_connection(m, {:system, "ctag-sys"})
    ProducerMonitor.observe_connection(m, {:content, "ctag-con"})
    sync(m)

    set_clock(clock, 1_000 + @inactivity + 1)
    tick(m)

    assert_receive {:status, %ProducerStatus{status: :recovering}}
    assert_receive {:recover_called, "pre", opts}
    assert opts[:after] == 1_000
  end

  test "connection startup has a bounded readiness deadline", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, "ctag-sys"})
    sync(m)
    state = :sys.get_state(m)

    set_clock(clock, 1_000 + @inactivity + 1)

    assert_raise RuntimeError,
                 "producer monitor connection readiness timed out; missing namespaces: [:content]",
                 fn -> ProducerMonitor.handle_info(:tick, state) end
  end

  test "resumed producer recovers incrementally when a namespace consumer tag changed across restart", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    set_clock(clock, 50_000)
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, "ctag-sys-new"})
    sync(m)
    refute_received {:recover_called, _, _}

    # A persisted content consumer tag is only a comparison baseline. Recovery waits
    # until the content pipeline has actually attached in this monitor session.
    ProducerMonitor.observe_connection(m, {:content, "ctag-con"})

    assert_receive {:status, %ProducerStatus{status: :recovering}}
    assert_receive {:recover_called, "pre", opts}
    assert opts[:after] == 1_000
  end

  test "a session change saved before a restart leaves producers unsynchronized", %{clock: clock} do
    persist_healthy_shutdown(1_000)

    # Simulate a crash after the session change is saved but before the
    # monitor visits producer 1 to prepare its recovery.
    store_snapshot(Snapshot.change_session(stored_snapshot(), %{system: "ctag-sys", content: "ctag-con-2"}))

    m = start_monitor(clock)
    assert {:ok, %ProducerStatus{status: :down}} = ProducerMonitor.producer(m, 1)

    ProducerMonitor.alive(m, 1, 1_000, true)
    sync(m)
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    refute_received {:recover_called, _, _}

    ProducerMonitor.observe_connection(m, {:system, "ctag-sys"})
    ProducerMonitor.observe_connection(m, {:content, "ctag-con-2"})

    assert_receive {:recover_called, "pre", opts}
    assert opts[:after] == 1_000
  end

  test "recovery intent is saved before the recovery request", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    set_clock(clock, 50_000)
    m = start_monitor([monitor_store: {RecordingStore, pid: self()}], clock)

    ProducerMonitor.observe_connection(m, {:system, "ctag-sys"})
    ProducerMonitor.observe_connection(m, {:content, "ctag-con"})
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
    set_clock(clock, 50_000)
    m = start_monitor([monitor_store: {FailingStore, []}], clock)
    ref = Process.monitor(m)

    ProducerMonitor.observe_connection(m, {:system, "ctag-sys"})
    ProducerMonitor.observe_connection(m, {:content, "ctag-con"})
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

  test "a missing snapshot starts every producer down", %{clock: clock} do
    m = start_monitor(clock)
    assert {:ok, %ProducerStatus{status: :down}} = ProducerMonitor.producer(m, 1)
    assert ETS.load([]) == nil
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
    refute resumable?(1)
  end

  test "unsubscribed alive remains pending until the second unchanged connection is ready", %{clock: clock} do
    persist_healthy_shutdown(1_000)
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, "ctag-sys"})
    ProducerMonitor.alive(m, 1, 1_000, false)
    sync(m)

    assert_receive {:status, %ProducerStatus{status: :recovering}}
    refute_received {:recover_called, _, _}
    refute resumable?(1)

    # The unchanged consumer tag does not itself indicate a reconnect. Becoming ready
    # must nevertheless issue the recovery remembered from subscribed=0.
    ProducerMonitor.observe_connection(m, {:content, "ctag-con"})
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

  test "persists delayed transitions", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    rid = assert_recovery_triggered()
    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %ProducerStatus{status: :up}}

    set_clock(clock, 20_000)
    ProducerMonitor.alive(m, 1, 20_000, true)
    ProducerMonitor.message(m, 1, 1_000)
    tick(m)
    assert_receive {:status, %ProducerStatus{status: :delayed}}

    assert synchronized?(1)
  end

  test "persists consumer tags as they change", %{clock: clock} do
    m = start_monitor(clock)

    ProducerMonitor.observe_connection(m, {:system, "ctag-a"})
    sync(m)
    assert stored_snapshot().consumer_tags == %{}

    ProducerMonitor.observe_connection(m, {:content, "ctag-b"})
    sync(m)
    assert stored_snapshot().consumer_tags == %{system: "ctag-a", content: "ctag-b"}

    ProducerMonitor.observe_connection(m, {:system, "ctag-c"})
    sync(m)
    assert stored_snapshot().consumer_tags == %{system: "ctag-c", content: "ctag-b"}
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

  test "a failover monitor starts passive until ownership is reported", %{clock: clock} do
    m = start_monitor([ownership: {:failover, :passive}], clock)

    ProducerMonitor.alive(m, 1, 1_000, true)
    sync(m)
    refute_receive {:recover_called, _product, _opts}
    assert ProducerMonitor.recover(m, 1, []) == {:error, :passive}

    report_active_state(m, :active)
    ProducerMonitor.alive(m, 1, 2_000, true)
    assert_recovery_triggered()
  end

  test "an always-active monitor ignores failover ownership reports", %{clock: clock} do
    ## State persistence ----------------------------------------------------------
    m = start_monitor([ownership: :always_active], clock)

    report_active_state(m, :passive)

    assert ProducerMonitor.recover(m, 1, full: true) == :ok
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
    m = start_monitor([ownership: {:failover, :passive}], clock)

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

    ProducerMonitor.observe_connection(m, {:system, "ctag-sys"})
    ProducerMonitor.observe_connection(m, {:content, "ctag-con"})
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
    ProducerMonitor.observe_connection(m, {:system, "s1"})
    ProducerMonitor.observe_connection(m, {:content, "c1"})
    rid = assert_recovery_triggered()
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    ProducerMonitor.snapshot_complete(m, 1, rid)
    assert_receive {:status, %ProducerStatus{status: :up}}
  end

  test "stale consumer tag observations after a reconnect do not re-trigger recovery", %{clock: clock} do
    m = start_monitor(clock)
    establish_session(m)

    # genuine reconnect: never-seen consumer tag
    ProducerMonitor.observe_connection(m, {:content, "c2"})
    assert_recovery_triggered()

    # straggler from the superseded session, delivered out of order
    ProducerMonitor.observe_connection(m, {:content, "c1"})
    sync(m)

    refute_receive {:recover_called, _product, _opts}
    assert stored_snapshot().consumer_tags == %{system: "s1", content: "c2"}
  end

  test "persisted consumer tags arm the restart connection gate before observations arrive", %{clock: clock} do
    save_state(%{}, [], %{system: "old-s", content: "old-c"})
    m = start_monitor(clock)

    # Persisted baselines identify a restart window. Recovery intent is durable,
    # but HTTP must wait until both current pipelines have reattached.
    ProducerMonitor.alive(m, 1, 1_000, true)
    sync(m)
    assert_receive {:status, %ProducerStatus{status: :recovering}}
    refute_received {:recover_called, _, _}

    ProducerMonitor.observe_connection(m, {:system, "old-s"})
    sync(m)
    refute_received {:recover_called, _, _}

    ProducerMonitor.observe_connection(m, {:content, "old-c"})
    assert_recovery_triggered()
  end

  test "session observations are ignored while passive and re-detected on promotion", %{clock: clock} do
    m = start_monitor(clock)
    establish_session(m)
    report_active_state(m, :passive)

    ProducerMonitor.observe_connection(m, {:content, "c2"})
    sync(m)

    # nothing recorded, nothing acted on: the owner heals the shared feed
    refute_receive {:recover_called, _product, _opts}
    refute_receive {:status, _producer}
    assert {:ok, %ProducerStatus{status: :up}} = ProducerMonitor.producer(m, 1)
    assert stored_snapshot().consumer_tags == %{system: "s1", content: "c1"}
    assert synchronized?(1)

    # the stale baseline re-detects the session change once promoted
    report_active_state(m, :active)
    ProducerMonitor.observe_connection(m, {:content, "c2"})
    assert_recovery_triggered()
    sync(m)
    assert stored_snapshot().consumer_tags == %{system: "s1", content: "c2"}
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

  ## Connection sessions -------------------------------------------------------

  ## Timer and checkpoint hygiene ---------------------------------------------
end

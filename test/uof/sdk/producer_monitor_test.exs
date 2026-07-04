defmodule UOF.SDK.ProducerMonitorTest do
  use ExUnit.Case, async: false

  alias UOF.SDK.Producer
  alias UOF.SDK.ProducerMonitor
  alias UOF.SDK.ProducerRegistry

  @inactivity 10_000

  defmodule Handler do
    @moduledoc false
    use UOF.SDK.MessageHandler

    @impl true
    def handle_producer_status(producer) do
      send(Application.fetch_env!(:betradar_uof_sdk, :test_pid), {:status, producer})
      :ok
    end
  end

  setup do
    Application.put_env(:betradar_uof_sdk, :test_pid, self())
    start_supervised!(ProducerRegistry)

    clock = start_supervised!(%{id: :clock, start: {Agent, :start_link, [fn -> 1_000 end]}})
    test_pid = self()

    monitor =
      start_supervised!(
        {ProducerMonitor,
         producers: [%Producer{id: 1, product: "pre"}],
         handler: Handler,
         recover: fn p -> send(test_pid, {:recover, p.id}) end,
         now_fun: fn -> Agent.get(clock, & &1) end,
         inactivity_ms: @inactivity,
         tick_ms: 60_000}
      )

    %{monitor: monitor, clock: clock}
  end

  defp set_clock(clock, value), do: Agent.update(clock, fn _ -> value end)

  defp tick(monitor),
    do:
      (
        send(monitor, :tick)
        GenServer.call(monitor, :sync)
      )

  defp sync(monitor), do: GenServer.call(monitor, :sync)

  test "first alive triggers recovery; completion brings the producer up", %{monitor: m} do
    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_receive {:recover, 1}
    sync(m)
    assert {:ok, %Producer{down?: true, recovering?: true}} = ProducerRegistry.get(1)

    ProducerMonitor.recovery_completed(m, 1, 99)
    assert_receive {:status, %Producer{id: 1, down?: false, reason: :first_recovery_completed}}
  end

  test "silence (alive interval violation) marks down and recovers", %{monitor: m, clock: clock} do
    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_receive {:recover, 1}
    ProducerMonitor.recovery_completed(m, 1, 1)
    assert_receive {:status, %Producer{down?: false}}

    # advance past inactivity with no new alive
    set_clock(clock, 1_000 + @inactivity + 1)
    tick(m)

    assert_receive {:status, %Producer{down?: true, reason: :alive_interval_violation}}
    assert_receive {:recover, 1}

    # second completion -> returned_from_inactivity (not first_recovery_completed)
    ProducerMonitor.recovery_completed(m, 1, 2)
    assert_receive {:status, %Producer{down?: false, reason: :returned_from_inactivity}}
  end

  test "processing lag marks down + delayed without recovering", %{monitor: m, clock: clock} do
    # bring it up first
    ProducerMonitor.alive(m, 1, 1_000, true)
    assert_receive {:recover, 1}
    ProducerMonitor.recovery_completed(m, 1, 1)
    assert_receive {:status, %Producer{down?: false}}

    # a content message processed at t=1000, then a *fresh* alive at t=20000
    ProducerMonitor.message(m, 1, 1_000)
    set_clock(clock, 20_000)
    ProducerMonitor.alive(m, 1, 20_000, true)
    sync(m)

    tick(m)
    assert_receive {:status, %Producer{down?: true, delayed?: true, reason: :processing_queue_delay_violation}}
    refute_received {:recover, 1}

    # processing catches up -> stabilized, back up
    ProducerMonitor.message(m, 1, 20_000)
    tick(m)
    assert_receive {:status, %Producer{down?: false, delayed?: false, reason: :processing_queue_delay_stabilized}}
  end

  test "observing a new connection recovers; same connection is deduped", %{monitor: m} do
    # initial connection -> recover
    ProducerMonitor.observe_connection(m, :conn_a)
    assert_receive {:recover, 1}
    ProducerMonitor.recovery_completed(m, 1, 1)
    assert_receive {:status, %Producer{down?: false}}

    # same connection pid -> deduped, no recovery
    ProducerMonitor.observe_connection(m, :conn_a)
    sync(m)
    refute_received {:recover, 1}

    # new connection pid (a reconnect) -> down + recover
    ProducerMonitor.observe_connection(m, :conn_b)
    assert_receive {:status, %Producer{down?: true, reason: :connection_down}}
    assert_receive {:recover, 1}
  end
end

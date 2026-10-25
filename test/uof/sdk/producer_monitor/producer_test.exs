defmodule UOF.SDK.ProducerMonitor.ProducerTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.ProducerMonitor.Producer

  @now 1_000_000_000_000

  test "a first subscribed alive requests initial synchronization" do
    producer = %Producer{id: 1, status: :down}

    assert {:recovery_needed, observed, :initial_sync} =
             Producer.observe_alive(producer, 900, true, 1_000)

    assert observed.last_alive_at == 1_000
  end

  test "an unsubscribed alive requests recovery" do
    producer = %Producer{id: 1, status: :up}

    assert {:recovery_needed, _producer, :unsubscribed} =
             Producer.observe_alive(producer, 900, false, 1_000)
  end

  test "a healthy subscribed alive advances the checkpoint" do
    producer = %Producer{id: 1, status: :up}

    assert {:checkpoint, observed, 900} = Producer.observe_alive(producer, 900, true, 1_000)
    assert observed.last_alive_at == 1_000
  end

  test "an existing recovery suppresses new alive decisions" do
    producer =
      %Producer{id: 1, status: :up}
      |> Producer.configure_recovery([])
      |> Producer.prepare_recovery(500)

    assert {:ok, observed} = Producer.observe_alive(producer, 900, false, 1_000)
    assert observed.last_alive_at == 1_000
  end

  test "a heartbeat gap requests recovery" do
    stale_alive = %Producer{id: 1, status: :up, last_alive_at: 100}

    assert {:recovery_needed, ^stale_alive, :alive_timeout} = Producer.check(stale_alive, 1_001, 900, true)
    assert :unchanged = Producer.check(%{stale_alive | last_alive_at: 1_000}, 1_001, 900, true)
  end

  test "a resuming producer becomes up only after a current heartbeat and the session gate" do
    producer = %Producer{id: 1, status: :resuming, last_alive_at: 1_000, alive_delay: 0}

    assert :unchanged = Producer.check(producer, 1_001, 900, false)
    assert :unchanged = Producer.check(%{producer | last_alive_at: nil}, 1_001, 900, true)
    assert {:transition, %Producer{status: :up}} = Producer.check(producer, 1_001, 900, true)

    # An alive drained from backlog keeps the producer alive but is not current.
    stale = %{producer | alive_delay: 901}
    assert :unchanged = Producer.check(stale, 1_001, 900, true)
  end

  test "an alive records how late it was read" do
    assert %Producer{alive_delay: 250} = elem(Producer.observe_alive(%Producer{id: 1, status: :up}, 750, true, 1_000), 1)
    assert %Producer{alive_delay: nil} = elem(Producer.observe_alive(%Producer{id: 1, status: :up}, nil, true, 1_000), 1)
  end

  @tag :capture_log
  test "request ids continue from their start and wrap within the request-id range" do
    producer =
      %Producer{id: 1}
      |> Producer.configure_recovery(
        recover_fun: fn _product, _opts -> {:ok, nil} end,
        request_id_start: 999_999,
        monotonic_fun: fn -> System.unique_integer([:monotonic]) end,
        max_recovery_ms: 0
      )
      |> Producer.prepare_recovery(nil)
      |> Producer.advance(0, 1_000)

    # Each stall reissues the job with the producer's next request id.
    issued =
      Enum.scan(1..3, producer, fn _, producer ->
        producer |> Producer.handle_response(:ok) |> Producer.advance(0, 1_000)
      end)

    assert Enum.map([producer | issued], & &1.recovery.job.request_id) == [999_999, 1_000_000, 1, 2]
  end

  test "default request-id sequences start at a random offset" do
    starts = Enum.map(1..5, fn _ -> Producer.configure_recovery(%Producer{id: 1}, []).recovery.next_request_id end)

    assert Enum.all?(starts, &(&1 in 1..1_000_000))
    assert length(Enum.uniq(starts)) > 1
  end

  test "the producer owns recovery HTTP attempts, state, and cooldown" do
    test_pid = self()

    recovery_opts = [
      recover_fun: fn product, opts ->
        send(test_pid, {:recover_called, product, opts})
        {:ok, :accepted}
      end,
      request_id_start: 42,
      monotonic_fun: fn -> 1_000 end,
      node_id: 7,
      min_interval_ms: 50,
      max_recovery_ms: 60_000
    ]

    producer =
      %Producer{id: 1}
      |> Producer.configure_recovery(recovery_opts)
      |> Producer.prepare_recovery(500)

    assert %Producer{recovery: %{job: %{after_ts: 500}}} = producer
    assert Producer.recovering?(producer)
    assert Producer.recovery_pending?(producer)

    producer = Producer.advance(producer, 0, 1_000)
    assert_receive {:recover_called, nil, request}
    assert request == [after: 0, node_id: 7, request_id: 42]
    refute Producer.recovery_pending?(producer)
    assert %Producer{recovery: %{job: %{request_id: 42}, last_issued_at: 1_000}} = producer

    # An accepted request is left alone until it stalls.
    producer = Producer.handle_response(producer, :ok)
    assert Producer.advance(producer, 0, 1_000) == producer

    assert {:ok, producer} = Producer.complete_recovery(producer, 42, 2_000)
    refute Producer.recovering?(producer)
    assert producer.status == :up
    assert producer.last_alive_at == 2_000

    # A new job within the cooldown since the last attempt stays pending.
    producer = producer |> Producer.prepare_recovery(500) |> Producer.advance(0, 1_000)
    assert Producer.recovery_pending?(producer)
    refute_received {:recover_called, _product, _request}
  end

  test "a pending job is not issued while the producer is silent" do
    test_pid = self()

    producer =
      %Producer{id: 1, last_alive_at: 0}
      |> Producer.configure_recovery(
        recover_fun: fn _product, _opts ->
          send(test_pid, :issued)
          {:ok, nil}
        end
      )
      |> Producer.prepare_recovery(500)

    # Last heard at 0: silent at 2_000 with a 1_000 heartbeat interval.
    assert Producer.silent?(producer, 2_000, 1_000)
    assert Producer.advance(producer, 2_000, 1_000) == producer

    # The next alive makes it current again, and the job goes out.
    {:ok, heard} = Producer.observe_alive(producer, 1_900, true, 2_000)
    refute Producer.silent?(heard, 2_000, 1_000)
    Producer.advance(heard, 2_000, 1_000)
    assert_receive :issued
  end

  test "a stalled request is reclamped when the recovery window moves" do
    test_pid = self()
    now = start_supervised!({Agent, fn -> @now end})
    telemetry_id = make_ref()

    :ok =
      :telemetry.attach(
        telemetry_id,
        [:uof_sdk, :recovery, :initiated],
        fn _event, _measurements, metadata, _config -> send(test_pid, {:initiated, metadata}) end,
        nil
      )

    on_exit(fn -> :telemetry.detach(telemetry_id) end)

    producer =
      %Producer{id: 1, recovery_window_minutes: 60}
      |> Producer.configure_recovery(
        recover_fun: fn _, opts ->
          send(test_pid, {:recover_called, opts})
          {:ok, :accepted}
        end,
        now_fun: fn -> Agent.get(now, & &1) end,
        monotonic_fun: fn -> Agent.get(now, & &1) end
      )
      |> Producer.prepare_recovery(@now - 2 * 60 * 60_000)
      |> Producer.advance(0, 1_000)

    assert_receive {:recover_called, first}
    assert first[:after] == @now - 50 * 60_000
    producer = Producer.handle_response(producer, :ok)
    assert_receive {:initiated, %{recovery_from: first_after}}
    assert first_after == first[:after]

    # Past max_recovery_ms (10 minutes by default), the request stalls.
    Agent.update(now, &(&1 + 20 * 60_000))
    producer = Producer.advance(producer, 0, 1_000)

    assert_receive {:recover_called, second}
    assert second[:after] == @now - 30 * 60_000
    Producer.handle_response(producer, :ok)
    assert_receive {:initiated, %{recovery_from: second_after}}
    assert second_after == second[:after]
  end

  test "a failed request is reclamped on retry" do
    test_pid = self()
    now = start_supervised!({Agent, fn -> @now end})

    producer =
      %Producer{id: 1, recovery_window_minutes: 60}
      |> Producer.configure_recovery(
        recover_fun: fn _, opts ->
          send(test_pid, {:recover_called, opts})
          {:ok, :accepted}
        end,
        now_fun: fn -> Agent.get(now, & &1) end,
        min_interval_ms: 0
      )
      |> Producer.prepare_recovery(@now - 2 * 60 * 60_000)
      |> Producer.advance(0, 1_000)

    assert_receive {:recover_called, first}
    assert first[:after] == @now - 50 * 60_000

    producer = Producer.handle_response(producer, {:error, :unavailable})
    Agent.update(now, &(&1 + 20 * 60_000))
    Producer.advance(producer, 0, 1_000)

    assert_receive {:recover_called, second}
    assert second[:after] == @now - 30 * 60_000
  end
end

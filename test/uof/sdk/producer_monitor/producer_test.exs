defmodule UOF.SDK.ProducerMonitor.ProducerTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.ProducerMonitor.Producer

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
      |> Producer.configure_recovery(recover_fun: fn _product, _opts -> {:ok, nil} end, request_id_start: 999_999)
      |> Producer.prepare_recovery(nil)
      |> Producer.initiate_recovery()

    # Each stall reissues the job with the producer's next request id.
    issued =
      Enum.scan(1..3, producer, fn _, producer ->
        Producer.handle_stall(producer, producer.recovery.job.request_id)
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

    producer = Producer.initiate_recovery(producer)
    assert_receive {:recover_called, nil, request}
    assert request == [after: 500, node_id: 7, request_id: 42]
    refute Producer.recovery_pending?(producer)
    assert %Producer{recovery: %{job: %{request_id: 42}, last_issued_at: 1_000}} = producer

    assert {:ok, producer} = Producer.complete_recovery(producer, 42, 2_000)
    refute Producer.recovering?(producer)
    assert producer.status == :up
    assert producer.last_alive_at == 2_000
  end
end

defmodule UOF.SDK.ProducerMonitor.ProducerTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.ProducerMonitor.Producer
  alias UOF.SDK.ProducerMonitor.Snapshot.Progress

  @now 1_000_000_000_000

  # A synchronized producer.
  defp up(fields \\ []) do
    %Producer{id: 1, pending_recovery: nil} |> struct(fields) |> Producer.configure_recovery([])
  end

  # A producer that owes a recovery and was last heard at 0.
  defp owing(opts, fields \\ []) do
    %Producer{id: 1, last_alive_at: 0} |> struct(fields) |> Producer.configure_recovery(opts)
  end

  test "restore resumes only a producer that owes no recovery and has a checkpoint" do
    producer = Producer.configure_recovery(%Producer{id: 1, last_alive_at: 5, alive_delay: 1}, [])

    assert %Producer{pending_recovery: nil, checkpoint: 2_000, resuming_since: 10, last_alive_at: nil, alive_delay: nil} =
             Producer.restore(producer, %Progress{checkpoint: 2_000, pending_recovery: nil}, 10)

    assert %Producer{pending_recovery: :incremental, resuming_since: nil} =
             Producer.restore(producer, %Progress{checkpoint: nil, pending_recovery: nil}, 10)

    for pending <- [:incremental, :full] do
      assert %Producer{pending_recovery: ^pending, checkpoint: 2_000, resuming_since: nil} =
               Producer.restore(producer, %Progress{checkpoint: 2_000, pending_recovery: pending}, 10)
    end

    assert Producer.restore(producer, %Progress{}, 10).pending_recovery == :incremental
  end

  test "a first alive makes the recovery a new producer owes issuable" do
    producer = Producer.configure_recovery(%Producer{id: 1}, [])
    assert Producer.status(producer, false) == :down

    observed = Producer.observe_alive(producer, 900, true, 1_000)
    assert observed.last_alive_at == 1_000
    assert observed.pending_recovery == :incremental
    assert Producer.status(observed, false) == :recovering
    assert Producer.status(observed, true) == :down
  end

  test "an unsubscribed alive owes a recovery, and a full one stays full" do
    assert %Producer{pending_recovery: :incremental} = Producer.observe_alive(up(), 900, false, 1_000)

    full = up(pending_recovery: :full)
    assert %Producer{pending_recovery: :full} = Producer.observe_alive(full, 900, false, 1_000)
  end

  test "subscribed alives advance the checkpoint only while up, and only forward" do
    producer = Producer.observe_alive(up(), 900, true, 1_000)
    assert producer.checkpoint == 900
    assert producer.last_alive_at == 1_000

    assert Producer.observe_alive(producer, 800, true, 1_100).checkpoint == 900
    assert Producer.observe_alive(producer, nil, true, 1_100).checkpoint == 900

    for fields <- [[pending_recovery: :incremental], [resuming_since: 0]] do
      assert Producer.observe_alive(up(fields), 900, true, 1_000).checkpoint == nil
    end
  end

  test "a heartbeat gap owes a recovery" do
    stale_alive = up(last_alive_at: 100)

    assert %Producer{pending_recovery: :incremental} = Producer.check(stale_alive, 1_001, 900)

    assert Producer.check(%{stale_alive | last_alive_at: 1_000}, 1_001, 900) == %{
             stale_alive
             | last_alive_at: 1_000
           }
  end

  test "a resuming producer becomes up only after a current heartbeat" do
    producer = up(resuming_since: 0, last_alive_at: 1_000, alive_delay: 0)
    assert Producer.status(producer, false) == :resuming

    assert %Producer{resuming_since: nil} = resumed = Producer.check(producer, 1_001, 900)
    assert Producer.status(resumed, false) == :up

    # An alive drained from backlog keeps the producer alive but is not current.
    stale = %{producer | alive_delay: 901}
    assert Producer.check(stale, 1_001, 900) == stale

    # Not heard within the interval of resuming: it owes a recovery.
    unheard = %{producer | last_alive_at: nil}
    assert %Producer{pending_recovery: :incremental} = Producer.check(unheard, 901, 900)
  end

  test "an alive records how late it was read" do
    assert %Producer{alive_delay: 250} = Producer.observe_alive(up(), 750, true, 1_000)
    assert %Producer{alive_delay: nil} = Producer.observe_alive(up(), nil, true, 1_000)
  end

  @tag :capture_log
  test "request ids continue from their start and wrap within the request-id range" do
    producer =
      [
        recover_fun: fn _product, _opts -> {:ok, nil} end,
        request_id_start: 999_999,
        monotonic_fun: fn -> System.unique_integer([:monotonic]) end,
        min_interval_ms: 0,
        max_recovery_ms: 0
      ]
      |> owing()
      |> Producer.advance(0, 1_000)

    # Each stall reissues the recovery with the producer's next request id.
    issued =
      Enum.scan(1..3, producer, fn _, producer ->
        producer |> Producer.handle_response(:ok) |> Producer.advance(0, 1_000)
      end)

    assert Enum.map([producer | issued], & &1.recovery.request.id) == [999_999, 1_000_000, 1, 2]
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

    producer = owing(recovery_opts, checkpoint: 60_500)
    assert Producer.recovering?(producer)

    producer = Producer.advance(producer, 0, 1_000)
    assert_receive {:recover_called, nil, request}
    assert request == [after: 500, node_id: 7, request_id: 42]
    assert %Producer{recovery: %{request: %{id: 42}, last_issued_at: 1_000}} = producer

    # An accepted request is left alone until it stalls.
    producer = Producer.handle_response(producer, :ok)
    assert Producer.advance(producer, 0, 1_000) == producer

    producer = Producer.complete_recovery(producer, 42, 2_000)
    refute Producer.recovering?(producer)
    assert Producer.status(producer, false) == :up
    assert producer.last_alive_at == 2_000
    assert producer.recovery.request == nil

    # A recovery owed within the cooldown since the last attempt waits.
    producer = producer |> Producer.owe(:incremental) |> Producer.advance(2_000, 1_000)
    assert producer.recovery.request == nil
    refute_received {:recover_called, _product, _request}
  end

  test "a full recovery omits the checkpoint" do
    test_pid = self()

    [recover_fun: fn _product, opts -> send(test_pid, {:recover_called, opts}) && {:ok, nil} end]
    |> owing(
      checkpoint: 60_500,
      pending_recovery: :full
    )
    |> Producer.advance(0, 1_000)

    assert_receive {:recover_called, opts}
    refute Keyword.has_key?(opts, :after)
  end

  test "a recovery is not issued until the producer is heard" do
    test_pid = self()
    recover_fun = fn _product, _opts -> send(test_pid, :issued) && {:ok, nil} end

    # Never heard, or last heard at 0 and silent at 2_000 with a 1_000 interval.
    unheard = Producer.configure_recovery(%Producer{id: 1}, recover_fun: recover_fun)
    silent = owing([recover_fun: recover_fun], last_alive_at: 0)

    for producer <- [unheard, silent] do
      assert Producer.advance(producer, 2_000, 1_000) == producer
    end

    assert Producer.silent?(silent, 2_000, 1_000)
    assert Producer.status(silent, true) == :down

    # The next alive makes it current again, and the request goes out.
    heard = Producer.observe_alive(silent, 1_900, true, 2_000)
    refute Producer.silent?(heard, 2_000, 1_000)
    Producer.advance(heard, 2_000, 1_000)
    assert_receive :issued
  end

  @tag :capture_log
  test "an interrupted completion advances the checkpoint and repeats the recovery" do
    producer =
      [recover_fun: fn _product, _opts -> {:ok, nil} end, min_interval_ms: 0]
      |> owing(checkpoint: 100)
      |> Producer.advance(0, 1_000)
      |> Producer.handle_response(:ok)

    request_id = producer.recovery.request.id

    producer =
      producer
      |> Producer.observe_alive(500, true, 0)
      |> Producer.observe_alive(600, false, 0)
      # After the interruption, subscribed alives no longer prove continuity.
      |> Producer.observe_alive(700, true, 0)
      |> Producer.complete_recovery(request_id, 0)

    assert %Producer{checkpoint: 500, pending_recovery: :incremental, recovery: %{request: nil}} = producer
    assert Producer.status(producer, false) == :recovering
  end

  test "a request in flight can be dropped; its completion is then stale" do
    producer =
      [recover_fun: fn _product, _opts -> {:ok, nil} end, min_interval_ms: 0]
      |> owing()
      |> Producer.advance(0, 1_000)

    request_id = producer.recovery.request.id
    dropped = Producer.drop_request(producer)

    assert dropped.recovery.request == nil
    assert Producer.complete_recovery(dropped, request_id, 0) == dropped
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
      [
        recover_fun: fn _, opts ->
          send(test_pid, {:recover_called, opts})
          {:ok, :accepted}
        end,
        now_fun: fn -> Agent.get(now, & &1) end,
        monotonic_fun: fn -> Agent.get(now, & &1) end
      ]
      |> owing(
        recovery_window_minutes: 60,
        checkpoint: @now - 2 * 60 * 60_000
      )
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
      [
        recover_fun: fn _, opts ->
          send(test_pid, {:recover_called, opts})
          {:ok, :accepted}
        end,
        now_fun: fn -> Agent.get(now, & &1) end,
        min_interval_ms: 0
      ]
      |> owing(
        recovery_window_minutes: 60,
        checkpoint: @now - 2 * 60 * 60_000
      )
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

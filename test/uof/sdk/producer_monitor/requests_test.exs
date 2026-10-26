defmodule UOF.SDK.ProducerMonitor.RequestsTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.ProducerMonitor.Producer
  alias UOF.SDK.ProducerMonitor.Requests

  @now 1_000_000_000_000

  # A producer that owes a recovery and is heard at `now`.
  defp owing(now, fields \\ []), do: struct(%Producer{id: 1, last_alive_at: now}, fields)

  defp reporting_recover_fun(result \\ {:ok, :accepted}) do
    test_pid = self()

    fn product, opts ->
      send(test_pid, {:recover_called, product, opts})
      result
    end
  end

  test "sends a due request with its replay start, node id, and request id" do
    requests = Requests.new(recover_fun: reporting_recover_fun(), request_id_start: 42, node_id: 7)

    {requests, producer} = Requests.advance(requests, owing(@now, checkpoint: 60_500), @now, 1_000)

    assert_receive {:recover_called, nil, opts}
    assert opts == [after: 500, node_id: 7, request_id: 42]
    assert producer.request.id == 42
    assert requests.next_request_id == 43
  end

  test "a full recovery omits the replay start" do
    requests = Requests.new(recover_fun: reporting_recover_fun())

    Requests.advance(requests, owing(@now, checkpoint: 60_500, pending_recovery: :full), @now, 1_000)

    assert_receive {:recover_called, nil, opts}
    refute Keyword.has_key?(opts, :after)
  end

  test "a recovery within the cooldown since the last attempt waits" do
    requests = Requests.new(recover_fun: reporting_recover_fun(), monotonic_fun: fn -> 1_000 end, min_interval_ms: 50)
    producer = owing(@now, last_issued_at: 990)

    assert {^requests, ^producer} = Requests.advance(requests, producer, @now, 1_000)
    refute_received {:recover_called, _product, _opts}
  end

  @tag :capture_log
  test "request ids continue from their start and wrap within the request-id range" do
    requests =
      Requests.new(
        recover_fun: fn _product, _opts -> {:ok, nil} end,
        request_id_start: 999_999,
        monotonic_fun: fn -> System.unique_integer([:monotonic]) end,
        min_interval_ms: 0,
        max_recovery_ms: 0
      )

    {requests, producer} = Requests.advance(requests, owing(0), 0, 1_000)

    # Each stall reissues the recovery with the next request id.
    {_requests, _producer, ids} =
      Enum.reduce(1..3, {requests, producer, [producer.request.id]}, fn _, {requests, producer, ids} ->
        producer = Requests.handle_response(requests, producer, producer.request.id, :ok)
        {requests, producer} = Requests.advance(requests, producer, 0, 1_000)
        {requests, producer, ids ++ [producer.request.id]}
      end)

    assert ids == [999_999, 1_000_000, 1, 2]
  end

  test "default request-id sequences start at a random offset" do
    starts = Enum.map(1..5, fn _ -> Requests.new([]).next_request_id end)

    assert Enum.all?(starts, &(&1 in 1..1_000_000))
    assert length(Enum.uniq(starts)) > 1
  end

  test "a stalled request is reclamped when the recovery window moves" do
    test_pid = self()
    clock = start_supervised!({Agent, fn -> @now end})
    telemetry_id = make_ref()

    :ok =
      :telemetry.attach(
        telemetry_id,
        [:uof_sdk, :recovery, :initiated],
        fn _event, _measurements, metadata, pid ->
          if self() == pid, do: send(pid, {:initiated, metadata})
        end,
        test_pid
      )

    on_exit(fn -> :telemetry.detach(telemetry_id) end)

    fn ->
      :telemetry.execute([:uof_sdk, :recovery, :initiated], %{}, %{recovery_from: :another_test})
    end
    |> Task.async()
    |> Task.await()

    refute_received {:initiated, _metadata}

    requests = Requests.new(recover_fun: reporting_recover_fun(), monotonic_fun: fn -> Agent.get(clock, & &1) end)
    producer = owing(@now, recovery_window_minutes: 60, checkpoint: @now - 2 * 60 * 60_000)

    {requests, producer} = Requests.advance(requests, producer, @now, 1_000)
    assert_receive {:recover_called, nil, first}
    assert first[:after] == @now - 50 * 60_000
    producer = Requests.handle_response(requests, producer, producer.request.id, :ok)
    assert_receive {:initiated, %{recovery_from: first_after}}
    assert first_after == first[:after]

    # Past max_recovery_ms (10 minutes by default), the request stalls. The
    # producer is still heard.
    later = @now + 20 * 60_000
    Agent.update(clock, fn _ -> later end)
    {requests, producer} = Requests.advance(requests, %{producer | last_alive_at: later}, later, 1_000)

    assert_receive {:recover_called, nil, second}
    assert second[:after] == later - 50 * 60_000
    Requests.handle_response(requests, producer, producer.request.id, :ok)
    assert_receive {:initiated, %{recovery_from: second_after}}
    assert second_after == second[:after]
  end

  @tag :capture_log
  test "a failed request is reclamped on retry" do
    requests = Requests.new(recover_fun: reporting_recover_fun(), min_interval_ms: 0)
    producer = owing(@now, recovery_window_minutes: 60, checkpoint: @now - 2 * 60 * 60_000)

    {requests, producer} = Requests.advance(requests, producer, @now, 1_000)
    assert_receive {:recover_called, nil, first}
    assert first[:after] == @now - 50 * 60_000

    producer = Requests.handle_response(requests, producer, producer.request.id, {:error, :unavailable})
    later = @now + 20 * 60_000
    Requests.advance(requests, %{producer | last_alive_at: later}, later, 1_000)

    assert_receive {:recover_called, nil, second}
    assert second[:after] == later - 50 * 60_000
  end
end

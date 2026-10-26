defmodule UOF.SDK.ProducerMonitor.ProducerTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.ProducerMonitor.Producer
  alias UOF.SDK.ProducerMonitor.Snapshot.Progress

  # A synchronized producer.
  defp up(fields \\ []), do: struct(%Producer{id: 1, pending_recovery: nil}, fields)

  # A producer that owes a recovery and was last heard at 0.
  defp owing(fields \\ []), do: struct(%Producer{id: 1, last_alive_at: 0}, fields)

  # A request started at monotonic time 0, accepted unless still `:awaiting`
  # its HTTP response.
  defp in_flight(producer, request_id \\ 7, response \\ :accepted) do
    producer = Producer.start_request(producer, request_id, 500, make_ref(), 0)
    if response == :accepted, do: put_in(producer.request.task, nil), else: producer
  end

  test "restore resumes only a producer that owes no recovery and has a checkpoint" do
    producer = %Producer{id: 1, last_alive_at: 5, alive_delay: 1, request: %{id: 1}}

    assert %Producer{
             pending_recovery: nil,
             checkpoint: 2_000,
             restored_at: 10,
             last_alive_at: nil,
             alive_delay: nil,
             request: nil
           } = Producer.restore(producer, %Progress{checkpoint: 2_000, pending_recovery: nil}, 10)

    assert %Producer{pending_recovery: :incremental, restored_at: 10} =
             Producer.restore(producer, %Progress{checkpoint: nil, pending_recovery: nil}, 10)

    for pending <- [:incremental, :full] do
      assert %Producer{pending_recovery: ^pending, checkpoint: 2_000, restored_at: 10} =
               Producer.restore(producer, %Progress{checkpoint: 2_000, pending_recovery: pending}, 10)
    end

    assert Producer.restore(producer, %Progress{}, 10).pending_recovery == :incremental
  end

  test "a first alive makes the recovery a new producer owes due" do
    producer = %Producer{id: 1}
    refute Producer.due?(producer, 1_000, 0, 900, 0)
    assert Producer.status(producer, false) == :down

    observed = Producer.observe_alive(producer, 900, true, 1_000)
    assert observed.last_alive_at == 1_000
    assert observed.pending_recovery == :incremental
    assert Producer.heard?(observed, 1_000, 900)
    assert Producer.due?(observed, 1_000, 0, 900, 0)
    assert Producer.status(observed, true) == :recovering
  end

  test "an unsubscribed alive owes a recovery, and a full one stays full" do
    assert %Producer{pending_recovery: :incremental} = Producer.observe_alive(up(), 900, false, 1_000)
    assert %Producer{pending_recovery: :full} = Producer.observe_alive(up(pending_recovery: :full), 900, false, 1_000)
  end

  test "subscribed alives advance the checkpoint only while up, and only forward" do
    producer = Producer.observe_alive(up(), 900, true, 1_000)
    assert producer.checkpoint == 900
    assert producer.last_alive_at == 1_000

    assert Producer.observe_alive(producer, 800, true, 1_100).checkpoint == 900
    assert Producer.observe_alive(producer, nil, true, 1_100).checkpoint == 900

    for pending <- [:incremental, :full] do
      assert Producer.observe_alive(up(pending_recovery: pending), 900, true, 1_000).checkpoint == nil
    end
  end

  test "a heartbeat gap owes a recovery" do
    stale_alive = up(last_alive_at: 100)

    assert %Producer{pending_recovery: :incremental} = Producer.check(stale_alive, 1_001, 900)
    assert Producer.check(%{stale_alive | last_alive_at: 1_000}, 1_001, 900) == %{stale_alive | last_alive_at: 1_000}
  end

  test "a restored producer is down until heard, and owes a recovery if not heard in time" do
    restored = Producer.restore(%Producer{id: 1}, %Progress{checkpoint: 500, pending_recovery: nil}, 0)

    refute Producer.heard?(restored, 100, 900)
    assert Producer.status(restored, false) == :down

    # Any alive, even one drained from backlog long after it was generated.
    heard = Producer.observe_alive(restored, 600, true, 100_000)
    assert Producer.heard?(heard, 100_000, 900)
    assert Producer.status(heard, true) == :up
    assert heard.checkpoint == 600

    assert Producer.check(restored, 900, 900) == restored
    assert %Producer{pending_recovery: :incremental} = Producer.check(restored, 901, 900)
  end

  test "an alive records how late it was read" do
    assert %Producer{alive_delay: 250} = Producer.observe_alive(up(), 750, true, 1_000)
    assert %Producer{alive_delay: nil} = Producer.observe_alive(up(), nil, true, 1_000)
  end

  test "a request is due once owed, heard, idle, and past the cooldown" do
    assert Producer.due?(owing(), 0, 0, 1_000, 0)

    refute Producer.due?(up(last_alive_at: 0), 0, 0, 1_000, 0)
    refute Producer.due?(owing(last_alive_at: nil), 0, 0, 1_000, 0)
    refute Producer.due?(owing(), 2_000, 0, 1_000, 0)
    refute Producer.due?(in_flight(owing()), 0, 0, 1_000, 0)

    # The cooldown runs from the last attempt, in monotonic time.
    issued = owing(last_issued_at: 100)
    refute Producer.due?(issued, 0, 149, 1_000, 50)
    assert Producer.due?(issued, 0, 150, 1_000, 50)
  end

  test "a request replays from the checkpoint, or omits it for a full recovery" do
    assert Producer.replay_from(owing(checkpoint: 500)) == 500
    assert Producer.replay_from(owing(checkpoint: nil)) == nil
    assert Producer.replay_from(owing(checkpoint: 500, pending_recovery: :full)) == nil
  end

  @tag :capture_log
  test "an accepted request completes on its own request id" do
    ref = make_ref()
    producer = [checkpoint: 500] |> owing() |> Producer.start_request(42, 500, ref, 1_000)

    assert %{id: 42, after_ts: 500, task: ^ref} = producer.request
    assert producer.last_issued_at == 1_000
    assert Producer.awaiting_response?(producer, ref)

    producer = Producer.handle_response(producer, :ok, 1_200, 30_000)
    refute Producer.awaiting_response?(producer, ref)

    # A completion for another request is stale.
    assert Producer.complete_recovery(producer, 41, 2_000, 1_500) == producer

    # The checkpoint advances to the last subscribed alive seen while it ran.
    producer = Producer.observe_alive(producer, 900, true, 1_900)
    completed = Producer.complete_recovery(producer, 42, 2_000, 1_500)
    assert %Producer{pending_recovery: nil, request: nil, last_alive_at: 2_000, checkpoint: 900} = completed
    assert Producer.status(completed, true) == :up
  end

  @tag :capture_log
  test "a failed request is retried after a full cooldown from the failure" do
    producer = Producer.start_request(owing(), 42, nil, make_ref(), 0)
    failed = Producer.handle_response(producer, {:error, "boom"}, 5_000, 1_000)

    assert %Producer{request: nil, last_issued_at: 5_000, pending_recovery: :incremental} = failed
    refute Producer.due?(failed, 0, 5_999, 10_000, 1_000)
    assert Producer.due?(failed, 0, 6_000, 10_000, 1_000)
  end

  @tag :capture_log
  test "an accepted request without completion past max_recovery_ms is dropped" do
    awaiting = in_flight(owing(), 7, :awaiting)
    accepted = in_flight(owing())

    # A request whose HTTP response has not arrived yet is left alone.
    assert Producer.drop_stalled_request(awaiting, 10_000, 1_000) == awaiting
    assert Producer.drop_stalled_request(accepted, 1_000, 1_000) == accepted
    assert %Producer{request: nil, pending_recovery: :incremental} = Producer.drop_stalled_request(accepted, 1_001, 1_000)
  end

  @tag :capture_log
  test "an interrupted completion advances the checkpoint and repeats the recovery" do
    producer =
      [checkpoint: 100]
      |> owing()
      |> in_flight()
      |> Producer.observe_alive(500, true, 0)
      |> Producer.observe_alive(600, false, 0)
      # After the interruption, subscribed alives no longer prove continuity.
      |> Producer.observe_alive(700, true, 0)
      |> Producer.complete_recovery(7, 0, 0)

    assert %Producer{checkpoint: 500, pending_recovery: :incremental, request: nil} = producer
    assert Producer.status(producer, true) == :recovering
  end

  @tag :capture_log
  test "a heartbeat gap interrupts a request in flight once" do
    producer = [last_alive_at: 0] |> owing() |> in_flight()

    interrupted = Producer.check(producer, 901, 900)
    assert interrupted.request.interrupted
    assert Producer.check(interrupted, 2_000, 900) == interrupted
  end

  test "a request in flight can be dropped; its completion is then stale" do
    dropped = owing() |> in_flight() |> Producer.drop_request()

    assert dropped.request == nil
    assert Producer.complete_recovery(dropped, 7, 0, 0) == dropped
  end
end

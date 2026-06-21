defmodule UOF.SDK.RecoveryTest do
  use ExUnit.Case, async: false

  alias UOF.SDK.CheckpointStore
  alias UOF.SDK.Producer
  alias UOF.SDK.Recovery

  @now 1_000_000_000_000

  setup do
    start_supervised!(CheckpointStore.ETS)
    test_pid = self()
    counter = start_supervised!(%{id: :rid, start: {Agent, :start_link, [fn -> 1 end]}})

    start_opts = [
      checkpoint_store: CheckpointStore.ETS,
      node_id: 39,
      now_fun: fn -> @now end,
      gen_request_id: fn -> Agent.get_and_update(counter, fn n -> {n, n + 1} end) end,
      recover_fun: fn product, opts ->
        send(test_pid, {:recover_call, product, opts})
        {:ok, :accepted}
      end,
      on_complete: fn id, rid -> send(test_pid, {:completed, id, rid}) end
    ]

    %{start_opts: start_opts}
  end

  defp start_recovery(opts, overrides \\ []) do
    start_supervised!({Recovery, Keyword.merge(opts, overrides)})
  end

  defp producer(attrs \\ []), do: struct(%Producer{id: 1, product: "pre"}, attrs)

  test "full recovery (no checkpoint) omits :after", %{start_opts: opts} do
    r = start_recovery(opts)
    Recovery.request(r, producer())

    assert_receive {:recover_call, "pre", call_opts}
    assert call_opts[:request_id] == 1
    assert call_opts[:node_id] == 39
    refute Keyword.has_key?(call_opts, :after)
  end

  test "incremental recovery uses the checkpoint as :after", %{start_opts: opts} do
    CheckpointStore.ETS.put(1, @now - 60_000)
    r = start_recovery(opts)
    Recovery.request(r, producer())

    assert_receive {:recover_call, "pre", call_opts}
    assert call_opts[:after] == @now - 60_000
  end

  test "clamps :after to the producer's recovery window", %{start_opts: opts} do
    # checkpoint is 2h old, window is 60 min -> clamp to now - 60 min
    CheckpointStore.ETS.put(1, @now - 2 * 60 * 60_000)
    r = start_recovery(opts)
    Recovery.request(r, producer(recovery_window_minutes: 60))

    assert_receive {:recover_call, "pre", call_opts}
    assert call_opts[:after] == @now - 60 * 60_000
  end

  test "keeps a single in-flight recovery per producer", %{start_opts: opts} do
    r = start_recovery(opts)
    Recovery.request(r, producer())
    Recovery.request(r, producer())

    assert_receive {:recover_call, "pre", _}
    refute_received {:recover_call, "pre", _}
  end

  test "matching snapshot_complete notifies and clears", %{start_opts: opts} do
    r = start_recovery(opts)
    Recovery.request(r, producer())
    assert_receive {:recover_call, "pre", call_opts}
    rid = call_opts[:request_id]

    Recovery.snapshot_complete(r, 1, rid)
    assert_receive {:completed, 1, ^rid}

    # cleared -> a new request initiates again
    Recovery.request(r, producer())
    assert_receive {:recover_call, "pre", _}
  end

  test "non-matching snapshot_complete is ignored", %{start_opts: opts} do
    r = start_recovery(opts)
    Recovery.request(r, producer())
    assert_receive {:recover_call, "pre", _}

    Recovery.snapshot_complete(r, 1, 9999)
    refute_receive {:completed, 1, _}, 100
  end

  test "stall reissues with the original timestamp and a new request id", %{start_opts: opts} do
    CheckpointStore.ETS.put(1, @now - 60_000)
    r = start_recovery(opts, max_recovery_ms: 20)
    Recovery.request(r, producer())

    assert_receive {:recover_call, "pre", first}
    assert_receive {:recover_call, "pre", second}, 500
    assert second[:after] == first[:after]
    assert second[:request_id] != first[:request_id]
  end

  test "API failure schedules a retry that re-initiates", %{start_opts: opts} do
    test_pid = self()
    attempts = start_supervised!(%{id: :attempts, start: {Agent, :start_link, [fn -> 0 end]}})

    flaky = fn product, call_opts ->
      n = Agent.get_and_update(attempts, fn n -> {n, n + 1} end)
      send(test_pid, {:recover_call, product, call_opts})
      if n == 0, do: {:error, :boom}, else: {:ok, :accepted}
    end

    r = start_recovery(opts, recover_fun: flaky, min_interval_ms: 10)
    Recovery.request(r, producer())

    assert_receive {:recover_call, "pre", _}, 200
    assert_receive {:recover_call, "pre", _}, 500
    assert Agent.get(attempts, & &1) >= 2
  end
end

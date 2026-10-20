defmodule UOF.SDK.ProducerMonitor.SnapshotTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.ProducerMonitor.Snapshot
  alias UOF.SDK.ProducerMonitor.Snapshot.Progress

  @snapshot %Snapshot{
    consumer_tags: %{system: "ctag-s", content: "ctag-c"},
    producers: %{
      1 => %Progress{checkpoint: 2_000, synchronized: true},
      3 => %Progress{checkpoint: nil, synchronized: false}
    }
  }

  test "round-trips through its encoding" do
    assert @snapshot |> Snapshot.encode() |> Snapshot.decode!() == @snapshot
  end

  test "resume requires a synchronized producer with a checkpoint" do
    assert Snapshot.resumable?(@snapshot, 1)
    refute Snapshot.resumable?(@snapshot, 3)
    refute Snapshot.resumable?(@snapshot, 99)

    unchecked = Snapshot.put_progress(@snapshot, 4, %Progress{synchronized: true})
    refute Snapshot.resumable?(unchecked, 4)
  end

  test "a session change unsynchronizes every producer and keeps checkpoints" do
    changed = Snapshot.change_session(@snapshot, %{system: "ctag-s", content: "ctag-c2"})

    assert changed.consumer_tags == %{system: "ctag-s", content: "ctag-c2"}
    assert Snapshot.progress(changed, 1) == %Progress{checkpoint: 2_000, synchronized: false}
    refute Snapshot.resumable?(changed, 1)
  end

  test "rejects binaries that are not external terms" do
    assert_raise ArgumentError, ~r/not a valid external term/, fn -> Snapshot.decode!("garbage") end
  end

  test "rejects other terms and unsupported versions" do
    assert_raise ArgumentError, ~r/unreadable producer monitor snapshot/, fn ->
      Snapshot.decode!(:erlang.term_to_binary(%{consumer_tags: %{}}))
    end

    assert_raise ArgumentError, ~r/unsupported producer monitor snapshot version: 2/, fn ->
      Snapshot.decode!(:erlang.term_to_binary({:uof_sdk_monitor_snapshot, 2, %{}}))
    end
  end

  test "rejects snapshots with an invalid shape" do
    assert_raise ArgumentError, ~r/invalid producer monitor snapshot consumer tag/, fn ->
      Snapshot.decode!(
        :erlang.term_to_binary(
          {:uof_sdk_monitor_snapshot, 1, %{consumer_tags: %{other: "t"}, producers: %{}, descriptions: []}}
        )
      )
    end

    assert_raise ArgumentError, ~r/invalid producer monitor snapshot progress/, fn ->
      Snapshot.decode!(
        :erlang.term_to_binary(
          {:uof_sdk_monitor_snapshot, 1, %{consumer_tags: %{}, producers: %{1 => {"2000", true}}, descriptions: []}}
        )
      )
    end
  end

  test "round-trips producer descriptions" do
    descriptions = [%{id: 1, name: "Live Odds", product: "liveodds", recovery_window_minutes: 4320}]
    snapshot = %Snapshot{descriptions: descriptions}

    assert %Snapshot{descriptions: ^descriptions} = snapshot |> Snapshot.encode() |> Snapshot.decode!()
  end

  test "rejects an invalid producer description" do
    assert_raise ArgumentError, ~r/invalid producer monitor snapshot description/, fn ->
      Snapshot.decode!(
        :erlang.term_to_binary(
          {:uof_sdk_monitor_snapshot, 1, %{consumer_tags: %{}, producers: %{}, descriptions: [%{id: 1}]}}
        )
      )
    end
  end

  test "rejects a snapshot missing a field" do
    assert_raise ArgumentError, ~r/invalid producer monitor snapshot: /, fn ->
      Snapshot.decode!(:erlang.term_to_binary({:uof_sdk_monitor_snapshot, 1, %{consumer_tags: %{}, producers: %{}}}))
    end
  end
end

defmodule UOF.SDK.ProducerMonitor.SnapshotTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.ProducerMonitor.Snapshot
  alias UOF.SDK.ProducerMonitor.Snapshot.Progress

  @snapshot %Snapshot{
    consumer_tag: "ctag-s",
    producers: %{
      1 => %Progress{checkpoint: 2_000, pending_recovery: nil},
      3 => %Progress{checkpoint: nil, pending_recovery: :full}
    }
  }

  test "round-trips through its encoding" do
    assert @snapshot |> Snapshot.encode() |> Snapshot.decode!() == @snapshot
  end

  test "rejects an old progress shape" do
    previous =
      :erlang.term_to_binary(
        {:uof_sdk_monitor_snapshot, 1, %{consumer_tag: "ctag-s", producers: %{1 => {2_000, true}}, descriptions: []}}
      )

    assert_raise ArgumentError, ~r/invalid producer monitor snapshot progress/, fn -> Snapshot.decode!(previous) end

    # The shape with separate synchronized and full-recovery flags.
    flags =
      :erlang.term_to_binary(
        {:uof_sdk_monitor_snapshot, 1,
         %{consumer_tag: "ctag-s", producers: %{1 => {2_000, true, false}}, descriptions: []}}
      )

    assert_raise ArgumentError, ~r/invalid producer monitor snapshot progress/, fn -> Snapshot.decode!(flags) end
  end

  test "rejects binaries that are not external terms" do
    assert_raise ArgumentError, ~r/not a valid external term/, fn -> Snapshot.decode!("garbage") end
  end

  test "rejects other terms and unsupported versions" do
    assert_raise ArgumentError, ~r/unreadable producer monitor snapshot/, fn ->
      Snapshot.decode!(:erlang.term_to_binary(%{consumer_tag: nil}))
    end

    assert_raise ArgumentError, ~r/unsupported producer monitor snapshot version: 2/, fn ->
      Snapshot.decode!(:erlang.term_to_binary({:uof_sdk_monitor_snapshot, 2, %{}}))
    end
  end

  # Earlier unreleased builds stored one consumer tag per pipeline.
  test "rejects a snapshot with per-pipeline consumer tags" do
    assert_raise ArgumentError, ~r/invalid producer monitor snapshot: /, fn ->
      Snapshot.decode!(
        :erlang.term_to_binary(
          {:uof_sdk_monitor_snapshot, 1, %{consumer_tags: %{system: "s", content: "c"}, producers: %{}, descriptions: []}}
        )
      )
    end
  end

  test "rejects snapshots with an invalid shape" do
    assert_raise ArgumentError, ~r/invalid producer monitor snapshot: /, fn ->
      Snapshot.decode!(
        :erlang.term_to_binary(
          {:uof_sdk_monitor_snapshot, 1, %{consumer_tag: %{system: "t"}, producers: %{}, descriptions: []}}
        )
      )
    end

    assert_raise ArgumentError, ~r/invalid producer monitor snapshot progress/, fn ->
      Snapshot.decode!(
        :erlang.term_to_binary(
          {:uof_sdk_monitor_snapshot, 1, %{consumer_tag: nil, producers: %{1 => {"2000", true, false}}, descriptions: []}}
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
          {:uof_sdk_monitor_snapshot, 1, %{consumer_tag: nil, producers: %{}, descriptions: [%{id: 1}]}}
        )
      )
    end
  end

  test "rejects a snapshot missing a field" do
    assert_raise ArgumentError, ~r/invalid producer monitor snapshot: /, fn ->
      Snapshot.decode!(:erlang.term_to_binary({:uof_sdk_monitor_snapshot, 1, %{consumer_tag: nil, producers: %{}}}))
    end
  end
end

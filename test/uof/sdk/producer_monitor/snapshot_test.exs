defmodule UOF.SDK.ProducerMonitor.SnapshotTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.ProducerMonitor.Snapshot

  test "checkpoint advancement is monotonic" do
    snapshot = Snapshot.advance_checkpoint(%Snapshot{}, 1, 2_000)

    assert Snapshot.checkpoint(snapshot, 1) == 2_000
    assert Snapshot.advance_checkpoint(snapshot, 1, 1_000) == snapshot
  end

  test "a connection change atomically commits tokens and requires recovery" do
    snapshot = %Snapshot{
      checkpoints: %{1 => 2_000, 2 => 3_000},
      resumable_producers: MapSet.new([1, 2]),
      connection_tokens: %{system: "s1", content: "c1"}
    }

    changed =
      Snapshot.commit_connection_change(
        snapshot,
        %{system: "s1", content: "c2"},
        [1, 2]
      )

    assert changed.connection_tokens == %{system: "s1", content: "c2"}
    refute Snapshot.resumable?(changed, 1)
    refute Snapshot.resumable?(changed, 2)
    assert changed.checkpoints == snapshot.checkpoints
  end

  test "synchronization eligibility is idempotent" do
    snapshot = Snapshot.mark_synchronized(%Snapshot{}, 1)

    assert Snapshot.resumable?(snapshot, 1)
    assert Snapshot.mark_synchronized(snapshot, 1) == snapshot
    refute Snapshot.resumable?(Snapshot.require_recovery(snapshot, 1), 1)
  end
end

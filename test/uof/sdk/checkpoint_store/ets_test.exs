defmodule UOF.SDK.CheckpointStore.ETSTest do
  use ExUnit.Case, async: false

  alias UOF.SDK.CheckpointStore.ETS

  setup do
    start_supervised!(ETS)
    :ok
  end

  test "get returns :none for an unknown producer" do
    assert ETS.get(99) == :none
  end

  test "put then get round-trips a timestamp" do
    assert ETS.put(1, 1_700_000_000_000) == :ok
    assert ETS.get(1) == {:ok, 1_700_000_000_000}
  end

  test "put overwrites the previous checkpoint" do
    ETS.put(3, 100)
    ETS.put(3, 200)
    assert ETS.get(3) == {:ok, 200}
  end

  test "delete drops the checkpoint" do
    ETS.put(3, 200)
    assert ETS.delete(3) == :ok
    assert ETS.get(3) == :none
  end
end

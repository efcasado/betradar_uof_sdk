defmodule UOF.SDK.ProducerMonitor.Store.ETSTest do
  use ExUnit.Case, async: false

  alias UOF.SDK.ProducerMonitor.Store.ETS

  setup do
    start_supervised!(ETS)
    :ok
  end

  test "loads nil until a snapshot is saved" do
    assert ETS.load([]) == nil
  end

  test "save replaces the snapshot" do
    assert ETS.save("first", []) == :ok
    assert ETS.save("second", []) == :ok
    assert ETS.load([]) == "second"
  end

  test "keys select independent snapshots" do
    ETS.save("a", key: :node_a)
    ETS.save("b", key: :node_b)

    assert ETS.load(key: :node_a) == "a"
    assert ETS.load(key: :node_b) == "b"
    assert ETS.load([]) == nil
  end
end

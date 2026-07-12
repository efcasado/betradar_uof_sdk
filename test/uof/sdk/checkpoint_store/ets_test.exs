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

  test "get_state returns an empty map when nothing is persisted" do
    assert ETS.get_state() == %{}
  end

  test "put_state then get_state round-trips producer states" do
    assert ETS.put_state(1, %{down?: false, delayed?: false}) == :ok
    assert ETS.put_state(3, %{down?: true, delayed?: true}) == :ok

    assert ETS.get_state() == %{
             1 => %{down?: false, delayed?: false},
             3 => %{down?: true, delayed?: true}
           }
  end

  test "put_state overwrites the previous state" do
    ETS.put_state(1, %{down?: false, delayed?: false})
    ETS.put_state(1, %{down?: true, delayed?: false})
    assert ETS.get_state() == %{1 => %{down?: true, delayed?: false}}
  end

  test "get_connection_tokens returns an empty map when nothing is persisted" do
    assert ETS.get_connection_tokens() == %{}
  end

  test "put_connection_token then get_connection_tokens round-trips tokens" do
    assert ETS.put_connection_token(:system, "amq.ctag-a") == :ok
    assert ETS.put_connection_token(:content, "amq.ctag-b") == :ok

    assert ETS.get_connection_tokens() == %{system: "amq.ctag-a", content: "amq.ctag-b"}
  end

  test "checkpoints, states and tokens do not collide" do
    ETS.put(1, 100)
    ETS.put_state(1, %{down?: false, delayed?: false})
    ETS.put_connection_token(:system, "amq.ctag-a")

    assert ETS.delete(1) == :ok
    assert ETS.get(1) == :none
    assert ETS.get_state() == %{1 => %{down?: false, delayed?: false}}
    assert ETS.get_connection_tokens() == %{system: "amq.ctag-a"}
  end
end

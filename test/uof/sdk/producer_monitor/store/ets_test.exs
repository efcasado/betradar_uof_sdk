defmodule UOF.SDK.ProducerMonitor.Store.ETSTest do
  use ExUnit.Case, async: false

  alias UOF.SDK.ProducerMonitor.Store.ConnectionState
  alias UOF.SDK.ProducerMonitor.Store.ETS
  alias UOF.SDK.ProducerMonitor.Store.ProducerState

  setup do
    start_supervised!(ETS)
    :ok
  end

  test "loads empty connection and producer state initially" do
    assert ETS.load_connection_state() == %ConnectionState{}
    assert ETS.load_producer_states() == %{}
  end

  test "a connection change atomically commits tokens and advances the generation" do
    first = ETS.commit_connection_change(%{system: "s1", content: "c1"})
    second = ETS.commit_connection_change(%{system: "s1", content: "c2"})

    assert first == %ConnectionState{tokens: %{system: "s1", content: "c1"}, generation: 1}
    assert second == %ConnectionState{tokens: %{system: "s1", content: "c2"}, generation: 2}
    assert ETS.load_connection_state() == second
  end

  test "producer operations preserve independent state and monotonic checkpoints" do
    assert ETS.advance_checkpoint(1, 2_000) == %ProducerState{checkpoint: 2_000}
    assert ETS.advance_checkpoint(1, 1_000) == %ProducerState{checkpoint: 2_000}

    assert ETS.mark_synchronized(1, 3) == %ProducerState{
             checkpoint: 2_000,
             synchronized_generation: 3
           }

    assert ETS.advance_checkpoint(2, 5_000) == %ProducerState{checkpoint: 5_000}
    assert ETS.require_recovery(1) == %ProducerState{checkpoint: 2_000}

    assert ETS.load_producer_states() == %{
             1 => %ProducerState{checkpoint: 2_000},
             2 => %ProducerState{checkpoint: 5_000}
           }
  end

  test "advancing the connection generation invalidates producers without rewriting them" do
    generation = ETS.commit_connection_change(%{system: "s1", content: "c1"}).generation
    ETS.advance_checkpoint(1, 2_000)
    producer = ETS.mark_synchronized(1, generation)

    assert ProducerState.resumable?(producer, generation)

    changed_generation = ETS.commit_connection_change(%{system: "s1", content: "c2"}).generation

    refute ProducerState.resumable?(producer, changed_generation)
    assert ETS.load_producer_states()[1] == producer
  end
end

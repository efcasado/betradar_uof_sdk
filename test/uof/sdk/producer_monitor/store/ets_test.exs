defmodule UOF.SDK.ProducerMonitor.Store.ETSTest do
  use ExUnit.Case, async: false

  alias UOF.SDK.ProducerMonitor.Store.ETS
  alias UOF.SDK.ProducerMonitor.Store.ProducerProgress
  alias UOF.SDK.ProducerMonitor.Store.Session

  setup do
    start_supervised!(ETS)
    :ok
  end

  test "loads an empty session and no producer progress initially" do
    assert ETS.load_session() == %Session{}
    assert ETS.load_producer_progress() == %{}
  end

  test "a session change atomically commits tokens and advances the generation" do
    first = ETS.commit_session_change(%{system: "s1", content: "c1"})
    second = ETS.commit_session_change(%{system: "s1", content: "c2"})

    assert first == %Session{tokens: %{system: "s1", content: "c1"}, generation: 1}
    assert second == %Session{tokens: %{system: "s1", content: "c2"}, generation: 2}
    assert ETS.load_session() == second
  end

  test "producer operations preserve independent state and monotonic checkpoints" do
    assert ETS.advance_checkpoint(1, 2_000) == %ProducerProgress{checkpoint: 2_000}
    assert ETS.advance_checkpoint(1, 1_000) == %ProducerProgress{checkpoint: 2_000}

    assert ETS.mark_synchronized(1, 3) == %ProducerProgress{
             checkpoint: 2_000,
             synchronized_generation: 3
           }

    assert ETS.advance_checkpoint(2, 5_000) == %ProducerProgress{checkpoint: 5_000}
    assert ETS.require_recovery(1) == %ProducerProgress{checkpoint: 2_000}

    assert ETS.load_producer_progress() == %{
             1 => %ProducerProgress{checkpoint: 2_000},
             2 => %ProducerProgress{checkpoint: 5_000}
           }
  end

  test "advancing the session generation invalidates producers without rewriting them" do
    generation = ETS.commit_session_change(%{system: "s1", content: "c1"}).generation
    ETS.advance_checkpoint(1, 2_000)
    producer = ETS.mark_synchronized(1, generation)

    assert ProducerProgress.resumable?(producer, generation)

    changed_generation = ETS.commit_session_change(%{system: "s1", content: "c2"}).generation

    refute ProducerProgress.resumable?(producer, changed_generation)
    assert ETS.load_producer_progress()[1] == producer
  end
end

defmodule UOF.SDK.ProducerMonitor.Store.ProducerStateTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.ProducerMonitor.Store.ProducerState

  test "resume requires a checkpoint synchronized in the current connection generation" do
    refute ProducerState.resumable?(%ProducerState{}, 1)

    refute ProducerState.resumable?(
             %ProducerState{checkpoint: 2_000, synchronized_generation: nil},
             1
           )

    refute ProducerState.resumable?(
             %ProducerState{checkpoint: 2_000, synchronized_generation: 1},
             2
           )

    assert ProducerState.resumable?(
             %ProducerState{checkpoint: 2_000, synchronized_generation: 2},
             2
           )
  end
end

defmodule UOF.SDK.ProducerMonitor.Store.ProducerProgressTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.ProducerMonitor.Store.ProducerProgress

  test "resume requires a checkpoint synchronized in the current session generation" do
    refute ProducerProgress.resumable?(%ProducerProgress{}, 1)

    refute ProducerProgress.resumable?(
             %ProducerProgress{checkpoint: 2_000, synchronized_generation: nil},
             1
           )

    refute ProducerProgress.resumable?(
             %ProducerProgress{checkpoint: 2_000, synchronized_generation: 1},
             2
           )

    assert ProducerProgress.resumable?(
             %ProducerProgress{checkpoint: 2_000, synchronized_generation: 2},
             2
           )
  end
end

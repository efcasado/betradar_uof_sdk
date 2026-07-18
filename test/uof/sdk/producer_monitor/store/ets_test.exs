defmodule UOF.SDK.ProducerMonitor.Store.ETSTest do
  use ExUnit.Case, async: false

  alias UOF.SDK.ProducerMonitor.Store.ETS
  alias UOF.SDK.ProducerMonitor.Store.Snapshot

  setup do
    start_supervised!(ETS)
    :ok
  end

  test "load returns an empty snapshot initially" do
    assert ETS.load() == %Snapshot{}
  end

  test "save atomically replaces the monitor snapshot" do
    first = %Snapshot{
      checkpoints: %{1 => 100},
      resumable_producers: MapSet.new([1]),
      connection_tokens: %{system: "ctag-a", content: "ctag-b"}
    }

    second = %Snapshot{
      checkpoints: %{1 => 200},
      resumable_producers: MapSet.new(),
      connection_tokens: %{system: "ctag-c", content: "ctag-b"}
    }

    assert ETS.save(first) == :ok
    assert ETS.load() == first

    assert ETS.save(second) == :ok
    assert ETS.load() == second
  end
end

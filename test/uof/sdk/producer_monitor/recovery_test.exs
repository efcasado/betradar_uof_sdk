defmodule UOF.SDK.ProducerMonitor.RecoveryTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.ProducerMonitor.Recovery

  test "a recovery is pending until a request is issued" do
    recovery = Recovery.new(1_000)

    assert Recovery.pending?(recovery)
    assert Recovery.matches_generation?(recovery, recovery.generation)

    in_flight = Recovery.in_flight(recovery, 42)

    refute Recovery.pending?(in_flight)
    refute Recovery.matches_generation?(in_flight, recovery.generation)
    assert Recovery.matches_request?(in_flight, 42)
  end

  test "returning to pending invalidates old timers and request completions" do
    recovery = Recovery.new(1_000)
    old_generation = recovery.generation
    recovery = recovery |> Recovery.in_flight(42) |> Recovery.pending()

    assert Recovery.pending?(recovery)
    refute Recovery.matches_generation?(recovery, old_generation)
    refute Recovery.matches_request?(recovery, 42)
  end
end

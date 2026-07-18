defmodule UOF.SDK.ProducerMonitor.HealthTest do
  use ExUnit.Case, async: true

  alias UOF.SDK.ProducerMonitor.Health
  alias UOF.SDK.ProducerMonitor.Producer

  test "a first subscribed alive requests initial synchronization" do
    producer = %Producer{id: 1, status: :down}

    assert {:recovery_needed, observed, :initial_sync} =
             Health.observe_alive(producer, 900, true, 1_000, false)

    assert observed.last_alive_at == 1_000
  end

  test "an unsubscribed alive requests recovery" do
    producer = %Producer{id: 1, status: :up}

    assert {:recovery_needed, _producer, :unsubscribed} =
             Health.observe_alive(producer, 900, false, 1_000, false)
  end

  test "a healthy subscribed alive advances the checkpoint" do
    producer = %Producer{id: 1, status: :up}

    assert {:checkpoint, observed, 900} =
             Health.observe_alive(producer, 900, true, 1_000, false)

    assert observed.last_alive_at == 1_000
  end

  test "an existing recovery suppresses new alive decisions" do
    producer = %Producer{id: 1, status: :recovering}

    assert {:ok, observed} = Health.observe_alive(producer, 900, false, 1_000, true)
    assert observed.last_alive_at == 1_000
  end

  test "health checks distinguish delivery gaps from local processing lag" do
    stale_alive = %Producer{id: 1, status: :up, last_alive_at: 100, last_message_timestamp: 1_000}

    assert {:recovery_needed, ^stale_alive, :alive_timeout} =
             Health.check(stale_alive, 1_001, 900, 900, true)

    delayed = %Producer{id: 1, status: :up, last_alive_at: 1_000, last_message_timestamp: 100}

    assert {:transition, %Producer{status: :delayed, processing_queue_delay: 901}} =
             Health.check(delayed, 1_001, 900, 900, true)
  end

  test "a resuming producer becomes up only after heartbeat, connection, and catch-up gates" do
    producer = %Producer{id: 1, status: :resuming, last_alive_at: 1_000, last_message_timestamp: 1_000}

    assert :unchanged = Health.check(producer, 1_001, 900, 900, false)
    assert {:transition, %Producer{status: :up}} = Health.check(producer, 1_001, 900, 900, true)
  end
end

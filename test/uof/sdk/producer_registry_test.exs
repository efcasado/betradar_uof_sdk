defmodule UOF.SDK.ProducerRegistryTest do
  use ExUnit.Case, async: false

  alias UOF.SDK.Producer
  alias UOF.SDK.ProducerRegistry

  setup do
    start_supervised!(ProducerRegistry)
    :ok
  end

  test "producers start down by default" do
    p = %Producer{id: 1, product: "liveodds"}
    assert p.down? == true
    assert Producer.up?(p) == false
  end

  test "register, get, and all (ordered by id)" do
    ProducerRegistry.register(%Producer{id: 3, product: "pre"})
    ProducerRegistry.register(%Producer{id: 1, product: "liveodds"})

    assert {:ok, %Producer{product: "pre"}} = ProducerRegistry.get(3)
    assert ProducerRegistry.get(99) == :error
    assert [%Producer{id: 1}, %Producer{id: 3}] = ProducerRegistry.all()
  end

  test "update mutates an existing producer and returns it" do
    ProducerRegistry.register(%Producer{id: 1, product: "liveodds"})

    assert {:ok, updated} =
             ProducerRegistry.update(1, fn p ->
               %{p | down?: false, reason: :first_recovery_completed}
             end)

    assert updated.down? == false
    assert Producer.up?(updated)
    assert {:ok, %Producer{reason: :first_recovery_completed}} = ProducerRegistry.get(1)
  end

  test "update on an unknown producer returns :error" do
    assert ProducerRegistry.update(42, & &1) == :error
  end

  test "UOF.SDK.producers/0 and producer/1 delegate to the registry" do
    ProducerRegistry.register(%Producer{id: 1, product: "liveodds"})

    assert [%Producer{id: 1}] = UOF.SDK.producers()
    assert {:ok, %Producer{id: 1}} = UOF.SDK.producer(1)
  end
end

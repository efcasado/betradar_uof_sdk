defmodule UOF.SDK.ConfigTest do
  use ExUnit.Case, async: true

  alias OffBroadway.Pulsar.Producer
  alias UOF.SDK.Config
  alias UOF.SDK.LogHandler

  @pulsar [host: "pulsar://localhost:6650", topic: "uof-feed", subscription: "uof-sdk"]

  defmodule CustomStore do
    @moduledoc false
    @behaviour UOF.SDK.ProducerMonitor.Store

    @impl true
    def load(_opts), do: nil
    @impl true
    def save(_snapshot, _opts), do: :ok
  end

  defmodule IncompleteStore do
    @moduledoc false
    def load(_opts), do: nil
  end

  defp load(overrides \\ []), do: Config.load(Keyword.merge([handler: LogHandler, pulsar: @pulsar], overrides))

  test "builds Pulsar producer specs from one topic and subscription" do
    config = load()

    assert {Producer, content_opts} = config.content_producer
    assert {Producer, system_opts} = config.system_producer
    assert {Pulsar.Client, client_opts} = config.pulsar_client

    assert client_opts == [name: :uof_sdk_pulsar, host: "pulsar://localhost:6650"]

    assert content_opts[:client] == :uof_sdk_pulsar
    refute Keyword.has_key?(content_opts, :host)
    assert content_opts[:topics] == ["uof-feed"]
    assert content_opts[:subscription] == "uof-sdk-content"
    assert content_opts[:consumer_opts][:subscription_type] == :key_shared

    assert system_opts[:client] == :uof_sdk_pulsar
    refute Keyword.has_key?(system_opts, :host)
    assert system_opts[:topics] == ["uof-feed"]
    assert system_opts[:subscription] == "uof-sdk-system"
    assert system_opts[:consumer_opts][:subscription_type] == :failover

    # Failover ownership reports gate the control plane; the Key_Shared
    # content subscription never emits them.
    assert system_opts[:active_state_callback] ==
             {UOF.SDK.ProducerMonitor, :active_state_change, []}

    refute Keyword.has_key?(content_opts, :active_state_callback)
  end

  test "defaults to the ETS monitor store" do
    assert load().monitor_store == {UOF.SDK.ProducerMonitor.Store.ETS, []}
  end

  test "accepts a custom monitor store" do
    assert load(monitor_store: CustomStore).monitor_store == {CustomStore, []}
    assert load(monitor_store: {CustomStore, key: "node-a"}).monitor_store == {CustomStore, key: "node-a"}
  end

  test "Pulsar consumers dead-letter failed messages after a single attempt by default" do
    config = load()

    for {Producer, opts} <- [config.content_producer, config.system_producer] do
      consumer_opts = opts[:consumer_opts]
      assert consumer_opts[:redelivery_interval] == 1_000
      assert consumer_opts[:dead_letter_policy] == [max_redelivery: 1]
      assert consumer_opts[:batch_index_ack_enabled] == true
    end
  end

  test "the content subscription starts at the earliest retained message by default" do
    config = load()

    {Producer, content_opts} = config.content_producer
    {Producer, system_opts} = config.system_producer
    assert content_opts[:consumer_opts][:initial_position] == :earliest
    refute Keyword.has_key?(system_opts[:consumer_opts], :initial_position)

    overridden = load(pulsar: @pulsar ++ [consumer_opts: [initial_position: :latest]])

    {Producer, content_opts} = overridden.content_producer
    assert content_opts[:consumer_opts][:initial_position] == :latest
  end

  test "user consumer options override the Pulsar failure defaults" do
    consumer_opts = [
      redelivery_interval: 5_000,
      dead_letter_policy: [max_redelivery: 3, topic: "uof-dlq"],
      subscription_type: :shared
    ]

    {Producer, opts} = load(pulsar: @pulsar ++ [consumer_opts: consumer_opts]).content_producer
    consumer_opts = opts[:consumer_opts]

    assert consumer_opts[:redelivery_interval] == 5_000
    assert consumer_opts[:dead_letter_policy] == [max_redelivery: 3, topic: "uof-dlq"]
    assert consumer_opts[:batch_index_ack_enabled] == true
    # The subscription type stays SDK-owned.
    assert consumer_opts[:subscription_type] == :key_shared
  end

  test "passes Pulsar :conn_opts through to the supervised client" do
    conn_opts = [socket_opts: [verify: :verify_none], request_timeout: 5_000]
    config = load(pulsar: @pulsar ++ [conn_opts: conn_opts])

    assert {Pulsar.Client, client_opts} = config.pulsar_client

    assert client_opts[:socket_opts] == [verify: :verify_none]
    assert client_opts[:request_timeout] == 5_000

    assert client_opts[:name] == :uof_sdk_pulsar
    assert client_opts[:host] == "pulsar://localhost:6650"

    assert {Producer, content_opts} = config.content_producer
    refute Keyword.has_key?(content_opts, :conn_opts)
    refute Keyword.has_key?(content_opts, :host)
  end

  test "requires Pulsar settings" do
    assert_raise NimbleOptions.ValidationError, ~r/required :pulsar option not found/, fn ->
      Config.load(handler: LogHandler)
    end
  end

  test "requires a Pulsar host, topic, and subscription" do
    for key <- [:host, :topic, :subscription] do
      assert_raise KeyError, fn -> load(pulsar: Keyword.delete(@pulsar, key)) end
    end
  end

  test "stores node_id when provided" do
    assert load(node_id: 42).node_id == 42
  end

  test "defaults the health thresholds" do
    defaults = load()
    assert defaults.inactivity_seconds == 20
    assert defaults.max_recovery_time == 600
  end

  test "defaults concurrency to 10 and accepts an override" do
    assert load().concurrency == 10
    assert load(concurrency: 50).concurrency == 50
  end

  test "raises on missing :handler" do
    assert_raise NimbleOptions.ValidationError, ~r/required :handler option not found/, fn ->
      Config.load(pulsar: @pulsar)
    end
  end

  test "rejects unknown options" do
    assert_raise NimbleOptions.ValidationError, ~r/unknown options \[:concurency\]/, fn ->
      load(concurency: 50)
    end
  end

  test "rejects options of the wrong type" do
    assert_raise NimbleOptions.ValidationError, ~r/:inactivity_seconds/, fn ->
      load(inactivity_seconds: "20")
    end

    assert_raise NimbleOptions.ValidationError, ~r/:node_id/, fn ->
      load(node_id: 0)
    end
  end

  test "rejects a handler that is not a loaded module" do
    assert_raise NimbleOptions.ValidationError, ~r/expected a loaded module implementing UOF.SDK.MessageHandler/, fn ->
      load(handler: MyApp.MissingHandler)
    end
  end

  test "rejects a monitor store missing required callbacks" do
    assert_raise NimbleOptions.ValidationError, ~r/IncompleteStore does not implement/, fn ->
      load(monitor_store: IncompleteStore)
    end
  end

  test "normalizes configured fallback producers" do
    config = load(producers: [[id: 1, product: "liveodds"]])

    assert config.producers == [%{id: 1, product: "liveodds", name: nil, recovery_window_minutes: nil}]
  end

  test "requires an id and product for configured producers" do
    assert_raise NimbleOptions.ValidationError, ~r/required :product option not found/, fn ->
      load(producers: [[id: 1]])
    end
  end
end

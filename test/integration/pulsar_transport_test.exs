defmodule UOF.SDK.PulsarTransportIntegrationTest do
  use ExUnit.Case, async: false

  alias UOF.Schemas.Feed
  alias UOF.SDK.ContentPipeline
  alias UOF.SDK.MessageHandler
  alias UOF.SDK.SystemPipeline
  alias UOF.SDK.TestSupport
  alias UOF.SDK.Transport

  @moduletag :integration
  @pulsar_port System.get_env("PULSAR_BROKER_PORT", "16650")
  @rabbitmq_port "RABBITMQ_AMQP_PORT" |> System.get_env("15672") |> String.to_integer()

  defmodule Handler do
    @moduledoc false
    use MessageHandler

    @impl true
    def handle_odds_change(message, context), do: notify({:odds_change, message, context})

    @impl true
    def handle_fixture_change(message, context), do: notify({:fixture_change, message, context})

    defp notify(message), do: send(Application.fetch_env!(:uof_sdk_test, :integration_test_pid), message)
  end

  defmodule Monitor do
    @moduledoc false

    def message(product, timestamp), do: notify({:observed_message, product, timestamp})

    def track_processing(product, timestamp, fun) do
      result = fun.()
      notify({:observed_message, product, timestamp})
      result
    end

    def observe_connection(tag), do: notify({:observed_connection, tag})

    defp notify(message), do: send(Application.fetch_env!(:uof_sdk_test, :integration_test_pid), message)
  end

  setup do
    Application.put_env(:uof_sdk_test, :integration_test_pid, self())
    TestSupport.start_http_client!()

    on_exit(fn -> Application.delete_env(:uof_sdk_test, :integration_test_pid) end)

    subscription = "uof-sdk-integration-#{System.unique_integer([:positive])}"

    %{
      children: [client],
      content: content_producer,
      system: system_producer,
      metadata_adapter: metadata_adapter
    } =
      Transport.producers(
        {:pulsar,
         host: "pulsar://localhost:#{@pulsar_port}",
         topic: "persistent://public/default/uof-feed",
         subscription: subscription},
        nil
      )

    start_supervised!(client)

    start_supervised!(
      {SystemPipeline,
       name: __MODULE__.SystemPipeline, producer: system_producer, metadata_adapter: metadata_adapter, monitor: Monitor}
    )

    start_supervised!(
      {ContentPipeline,
       name: __MODULE__.Pipeline,
       handler: Handler,
       concurrency: 1,
       producer: content_producer,
       metadata_adapter: metadata_adapter,
       monitor: Monitor,
       node_id: 7}
    )

    TestSupport.wait_for_subscription!("#{subscription}-system")
    TestSupport.wait_for_subscription!("#{subscription}-content")

    {:ok, connection} = AMQP.Connection.open(host: "localhost", port: @rabbitmq_port)
    {:ok, channel} = AMQP.Channel.open(connection)

    on_exit(fn -> AMQP.Connection.close(connection) end)

    %{channel: channel, subscription: subscription}
  end

  test "carries synthetic UOF XML and routing metadata from RabbitMQ to the SDK", %{channel: channel} do
    messages = [
      {"hi.-.pre.odds_change.1.sr:match.12345.-",
       ~s(<odds_change product="1" event_id="sr:match:12345" timestamp="42"/>)},
      {"hi.-.pre.fixture_change.1.sr:match.67890.-",
       ~s(<fixture_change product="1" event_id="sr:match:67890" timestamp="43" change_type="1"/>)}
    ]

    Enum.each(messages, fn {routing_key, xml} ->
      :ok = AMQP.Basic.publish(channel, "unifiedfeed", routing_key, xml)
    end)

    assert_receive {:odds_change, %Feed.OddsChange{} = odds_change, odds_context}, 10_000
    assert odds_change.event_id == "sr:match:12345"
    assert odds_context.routing_key == "hi.-.pre.odds_change.1.sr:match.12345.-"
    assert odds_context.event_urn == "sr:match:12345"

    assert_receive {:fixture_change, %Feed.FixtureChange{} = fixture_change, fixture_context}, 10_000
    assert fixture_change.event_id == "sr:match:67890"
    assert fixture_context.routing_key == "hi.-.pre.fixture_change.1.sr:match.67890.-"
    assert fixture_context.event_urn == "sr:match:67890"

    assert_receive {:observed_connection, {:content, consumer_tag}}, 10_000
    assert is_binary(consumer_tag) and consumer_tag != ""

    assert_receive {:observed_message, 1, 42}, 10_000
    assert_receive {:observed_message, 1, 43}, 10_000
  end

  test "keys messages by ordering scope, one key per batch entry", %{channel: channel} do
    probe = "uof-sdk-key-probe-#{System.unique_integer([:positive])}"
    TestSupport.create_probe_subscription!(probe)
    on_exit(fn -> TestSupport.delete_subscription(probe) end)

    messages = [
      {"hi.-.live.odds_change.1.sr:match.333.-", ~s(<odds_change product="1" event_id="sr:match:333" timestamp="1"/>)},
      {"hi.-.live.odds_change.1.sr:match.444.-", ~s(<odds_change product="1" event_id="sr:match:444" timestamp="2"/>)},
      {"hi.-.live.bet_stop.1.sr:match.333.-", ~s(<bet_stop product="1" event_id="sr:match:333" timestamp="3"/>)},
      {"-.-.-.alive.-.-.-.-", ~s(<alive product="1" timestamp="4" subscribed="1"/>)},
      {"-.-.-.snapshot_complete.-.-.-.7", ~s(<snapshot_complete product="1" request_id="1" timestamp="5"/>)}
    ]

    Enum.each(messages, fn {routing_key, xml} ->
      :ok = AMQP.Basic.publish(channel, "unifiedfeed", routing_key, xml)
    end)

    # odds_change and bet_stop for the same event share a key, so Key-Shared
    # delivers both to the same instance. All distinct keys surviving as entry
    # keys shows the source's KEY_BASED batcher never mixed them in one entry.
    assert_receive {:odds_change, _, %{routing_key: "hi.-.live.odds_change.1.sr:match.444.-"}}, 10_000

    assert await_entry_keys(probe, MapSet.new(["sr:match:333", "sr:match:444", "system"])) ==
             MapSet.new(["sr:match:333", "sr:match:444", "system"])
  end

  # The probe backlog fills asynchronously; settle on the expected keys or
  # return the last observation for the assertion message.
  defp await_entry_keys(subscription, expected, attempts \\ 50) do
    keys = subscription |> TestSupport.peek_entry_keys() |> MapSet.new()

    if keys == expected or attempts == 0 do
      keys
    else
      Process.sleep(100)
      await_entry_keys(subscription, expected, attempts - 1)
    end
  end

  test "drops another node's recovery replay", %{channel: channel} do
    other = "lo.pre.-.odds_change.1.sr:match.555.8"
    ours = "lo.pre.-.odds_change.1.sr:match.555.7"

    for routing_key <- [other, ours] do
      xml = ~s(<odds_change product="1" event_id="sr:match:555" timestamp="1"/>)
      :ok = AMQP.Basic.publish(channel, "unifiedfeed", routing_key, xml)
    end

    # Same event key, so both are handled in order by one processor: once ours
    # arrives, the other node's message has already been dropped.
    assert_receive {:odds_change, _, %{routing_key: ^ours}}, 10_000
    refute_received {:odds_change, _, %{routing_key: ^other}}
  end

  test "dead-letters a message that fails once instead of processing it again", %{
    channel: channel,
    subscription: subscription
  } do
    test_pid = self()
    id = make_ref()

    :telemetry.attach(
      id,
      [:uof_sdk, :message, :failed],
      fn _event, _measurements, metadata, _config -> send(test_pid, {:failed, metadata.routing_key}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)

    routing_key = "hi.-.live.odds_change.1.sr:match.777.-"
    :ok = AMQP.Basic.publish(channel, "unifiedfeed", routing_key, "<not valid UOF XML>")

    assert_receive {:failed, ^routing_key}, 10_000

    dlq = "uof-feed-#{subscription}-content-DLQ"
    assert TestSupport.await_topic_messages!(dlq, 1) == 1

    # Diverted on its redelivery, so the pipeline saw it exactly once.
    refute_receive {:failed, ^routing_key}, 2_000
  end

  test "changes the consumer tag when the source connector restarts", %{channel: channel} do
    publish_odds_change(channel, "111", 100)

    assert_receive {:odds_change, %Feed.OddsChange{}, _context}, 10_000
    assert_receive {:observed_connection, {:content, old_consumer_tag}}, 10_000

    {:ok, old_queue} = TestSupport.source_queue()
    TestSupport.restart_source!()
    _new_queue = TestSupport.wait_for_new_source_queue!(old_queue)

    publish_odds_change(channel, "222", 101)

    assert_receive {:odds_change, %Feed.OddsChange{}, _context}, 10_000
    assert_receive {:observed_connection, {:content, new_consumer_tag}}, 10_000

    refute new_consumer_tag == old_consumer_tag
  end

  defp publish_odds_change(channel, event_id, timestamp) do
    routing_key = "hi.-.pre.odds_change.1.sr:match.#{event_id}.-"
    xml = ~s(<odds_change product="1" event_id="sr:match:#{event_id}" timestamp="#{timestamp}"/>)

    :ok = AMQP.Basic.publish(channel, "unifiedfeed", routing_key, xml)
  end
end

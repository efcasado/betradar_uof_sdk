defmodule UOF.SDK.PulsarTransportIntegrationTest do
  use ExUnit.Case, async: false

  alias UOF.Schemas.Feed
  alias UOF.SDK.ContentPipeline
  alias UOF.SDK.MessageHandler
  alias UOF.SDK.Transport

  @moduletag :integration
  @pulsar_port System.get_env("PULSAR_BROKER_PORT", "16650")
  @pulsar_http_port System.get_env("PULSAR_HTTP_PORT", "18080")
  @rabbitmq_port "RABBITMQ_AMQP_PORT" |> System.get_env("15672") |> String.to_integer()

  defmodule Handler do
    @moduledoc false
    use MessageHandler

    @impl true
    def handle_odds_change(message, context), do: notify({:odds_change, message, context})

    @impl true
    def handle_fixture_change(message, context), do: notify({:fixture_change, message, context})

    defp notify(message), do: send(Application.fetch_env!(:uof_sdk, :integration_test_pid), message)
  end

  defmodule Monitor do
    @moduledoc false

    def message(product, timestamp), do: notify({:observed_message, product, timestamp})
    def observe_connection(token), do: notify({:observed_connection, token})

    defp notify(message), do: send(Application.fetch_env!(:uof_sdk, :integration_test_pid), message)
  end

  setup do
    Application.put_env(:uof_sdk, :integration_test_pid, self())
    {:ok, _apps} = Application.ensure_all_started(:inets)

    on_exit(fn -> Application.delete_env(:uof_sdk, :integration_test_pid) end)

    subscription = "uof-sdk-integration-#{System.unique_integer([:positive])}"

    %{content: producer, metadata_adapter: metadata_adapter} =
      Transport.producers(
        {:pulsar,
         host: "pulsar://localhost:#{@pulsar_port}",
         topic: "persistent://public/default/uof-feed",
         subscription: subscription,
         consumer_opts: [initial_position: :earliest]},
        nil
      )

    start_supervised!(
      {ContentPipeline,
       name: __MODULE__.Pipeline,
       handler: Handler,
       concurrency: 1,
       producer: producer,
       metadata_adapter: metadata_adapter,
       monitor: Monitor}
    )

    wait_for_subscription("#{subscription}-content")

    {:ok, connection} = AMQP.Connection.open(host: "localhost", port: @rabbitmq_port)
    {:ok, channel} = AMQP.Channel.open(connection)

    on_exit(fn -> AMQP.Connection.close(connection) end)

    %{channel: channel}
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

    assert_receive {:observed_connection, {:content, {queue_name, consumer_tag}}}, 10_000
    assert is_binary(queue_name) and queue_name != ""
    assert is_binary(consumer_tag) and consumer_tag != ""

    assert_receive {:observed_message, 1, 42}, 10_000
    assert_receive {:observed_message, 1, 43}, 10_000
  end

  defp wait_for_subscription(subscription, attempts \\ 100)

  defp wait_for_subscription(subscription, attempts) when attempts > 0 do
    url =
      ~c"http://localhost:#{@pulsar_http_port}/admin/v2/persistent/public/default/uof-feed/subscriptions"

    case :httpc.request(:get, {url, []}, [timeout: 1_000], body_format: :binary) do
      {:ok, {{_version, 200, _reason}, _headers, body}} ->
        if String.contains?(body, subscription) do
          :ok
        else
          Process.sleep(100)
          wait_for_subscription(subscription, attempts - 1)
        end

      _result ->
        Process.sleep(100)
        wait_for_subscription(subscription, attempts - 1)
    end
  end

  defp wait_for_subscription(subscription, 0) do
    flunk("Pulsar subscription #{subscription} did not become ready")
  end
end

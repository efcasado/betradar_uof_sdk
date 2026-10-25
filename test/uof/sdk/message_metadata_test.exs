defmodule UOF.SDK.MessageMetadataTest do
  use ExUnit.Case, async: true

  alias Broadway.Message
  alias UOF.SDK.MessageMetadata

  defp message(metadata), do: %Message{data: "", acknowledger: {Broadway.NoopAcknowledger, nil, nil}, metadata: metadata}

  test "reads the routing key from the connector's routing key property" do
    message = message(%{properties: %{"__rabbitmq_routing_key" => "hi.-.live.odds_change.1.sr:match.12345.-"}})

    assert MessageMetadata.routing_key(message) == "hi.-.live.odds_change.1.sr:match.12345.-"
  end

  test "an absent routing key property reads as empty" do
    assert MessageMetadata.routing_key(message(%{properties: %{}})) == ""
    assert MessageMetadata.routing_key(message(%{})) == ""
  end

  test "reads the upstream consumer tag from the connector's property" do
    assert MessageMetadata.consumer_tag(message(%{properties: %{"__rabbitmq_consumer_tag" => "amq.ctag-1"}})) ==
             "amq.ctag-1"

    assert MessageMetadata.consumer_tag(message(%{properties: %{}})) == nil
  end

  test "reports Pulsar delivery details for failure logs" do
    message = message(%{message_id_string: "12:3:-1", redelivery_count: 0})

    assert MessageMetadata.delivery_log_metadata(message) == [uof_message_id: "12:3:-1", uof_redelivery_count: 0]
  end
end

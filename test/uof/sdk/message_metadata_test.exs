defmodule UOF.SDK.MessageMetadataTest do
  use ExUnit.Case, async: true

  alias Broadway.Message
  alias UOF.SDK.MessageMetadata

  test "uses the consumer tag for built-in AMQP metadata" do
    message = %Message{
      data: "",
      acknowledger: {Broadway.NoopAcknowledger, nil, nil},
      metadata: %{consumer_tag: "amq.ctag-1"}
    }

    assert MessageMetadata.consumer_tag(message, :amqp) == "amq.ctag-1"
  end

  test "reads the Pulsar routing key from the connector's routing key property" do
    message = %Message{
      data: "",
      acknowledger: {Broadway.NoopAcknowledger, nil, nil},
      metadata: %{
        properties: %{"__rabbitmq_routing_key" => "hi.-.live.odds_change.1.sr:match.12345.-"}
      }
    }

    assert MessageMetadata.routing_key(message, :pulsar_rabbitmq_source, :routing_key) ==
             "hi.-.live.odds_change.1.sr:match.12345.-"
  end
end

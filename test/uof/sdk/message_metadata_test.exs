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

  test "reports each transport's own delivery details for failure logs" do
    amqp = %Message{
      data: "",
      acknowledger: {Broadway.NoopAcknowledger, nil, nil},
      metadata: %{redelivered: false, delivery_tag: 7}
    }

    pulsar = %Message{
      data: "",
      acknowledger: {Broadway.NoopAcknowledger, nil, nil},
      metadata: %{message_id_string: "12:3:-1", redelivery_count: 0}
    }

    assert MessageMetadata.delivery_log_metadata(amqp, :amqp) == [uof_redelivered: false, uof_delivery_tag: 7]

    assert MessageMetadata.delivery_log_metadata(pulsar, :pulsar_rabbitmq_source) ==
             [uof_message_id: "12:3:-1", uof_redelivery_count: 0]
  end
end

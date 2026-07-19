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

    assert MessageMetadata.connection_token(message, :amqp, nil) == "amq.ctag-1"
  end

  test "retains the connection pid fallback for custom AMQP producers" do
    pid = self()

    message = %Message{
      data: "",
      acknowledger: {Broadway.NoopAcknowledger, nil, nil},
      metadata: %{amqp_channel: %{conn: %{pid: pid}}}
    }

    assert MessageMetadata.connection_token(message, :amqp, nil) == pid
  end
end

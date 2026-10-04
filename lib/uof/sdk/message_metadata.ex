defmodule UOF.SDK.MessageMetadata do
  @moduledoc false

  alias Broadway.Message

  @type adapter :: :amqp | :pulsar_rabbitmq_source

  @spec routing_key(Message.t(), adapter(), atom()) :: binary()
  def routing_key(%Message{metadata: metadata}, :amqp, key) do
    case Map.get(metadata, key) do
      rk when is_binary(rk) -> rk
      _ -> ""
    end
  end

  def routing_key(%Message{metadata: metadata}, :pulsar_rabbitmq_source, _key) do
    case metadata[:key] do
      rk when is_binary(rk) -> rk
      _ -> ""
    end
  end

  # Both built-in adapters resolve the token to the AMQP consumer tag of the
  # consume session actually attached to Betradar: the app's own session for
  # AMQP, the RabbitMQ source connector's session for Pulsar. Server-generated
  # tags (`amq.ctag-…`) are unique per consume, so a changed tag is exactly "a
  # delivery gap was possible". Custom AMQP producers that do not expose the
  # tag retain the previous connection-pid fallback.
  @spec connection_token(Message.t(), adapter(), atom() | nil) :: term() | nil
  def connection_token(%Message{metadata: metadata} = message, :amqp, nil) do
    Map.get(metadata, :consumer_tag) || connection_pid(message)
  end

  def connection_token(%Message{metadata: metadata}, :amqp, key), do: Map.get(metadata, key)

  def connection_token(%Message{metadata: metadata}, :pulsar_rabbitmq_source, _key) do
    case metadata[:properties] do
      %{"__rabbitmq_consumer_tag" => consumer_tag} -> consumer_tag
      _properties -> nil
    end
  end

  # Custom AMQP producers may not opt in to BroadwayRabbitMQ's `:consumer_tag`
  # metadata. Preserve the previous reconnect token for those producers; a pid
  # cannot match across VM restarts, which safely forces recovery. This fallback
  # only detects connection replacement: custom producers sharing a connection
  # must expose :consumer_tag (or an explicit token) to detect channel reconnects.
  defp connection_pid(%Message{metadata: %{amqp_channel: %{conn: %{pid: pid}}}}), do: pid
  defp connection_pid(_message), do: nil
end

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
    case metadata[:properties] do
      %{"__rabbitmq_routing_key" => rk} when is_binary(rk) -> rk
      _properties -> ""
    end
  end

  @spec connection_token(Message.t(), adapter(), atom() | nil) :: term() | nil
  def connection_token(%Message{metadata: metadata}, :amqp, nil), do: Map.get(metadata, :consumer_tag)
  def connection_token(%Message{metadata: metadata}, :amqp, key), do: Map.get(metadata, key)

  def connection_token(%Message{metadata: metadata}, :pulsar_rabbitmq_source, _key) do
    case metadata[:properties] do
      %{"__rabbitmq_consumer_tag" => consumer_tag} -> consumer_tag
      _properties -> nil
    end
  end
end

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

  @spec consumer_tag(Message.t(), adapter()) :: binary() | nil
  def consumer_tag(%Message{metadata: metadata}, :amqp), do: Map.get(metadata, :consumer_tag)

  def consumer_tag(%Message{metadata: metadata}, :pulsar_rabbitmq_source) do
    case metadata[:properties] do
      %{"__rabbitmq_consumer_tag" => consumer_tag} -> consumer_tag
      _properties -> nil
    end
  end
end

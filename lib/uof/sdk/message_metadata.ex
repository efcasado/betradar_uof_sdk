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

  @doc false
  @spec delivery_log_metadata(Message.t(), adapter()) :: keyword()
  def delivery_log_metadata(%Message{metadata: metadata}, :amqp),
    do: [uof_redelivered: metadata[:redelivered], uof_delivery_tag: metadata[:delivery_tag]]

  def delivery_log_metadata(%Message{metadata: metadata}, :pulsar_rabbitmq_source),
    do: [uof_message_id: metadata[:message_id_string], uof_redelivery_count: metadata[:redelivery_count]]

  @spec consumer_tag(Message.t(), adapter()) :: binary() | nil
  def consumer_tag(%Message{metadata: metadata}, :amqp), do: Map.get(metadata, :consumer_tag)

  def consumer_tag(%Message{metadata: metadata}, :pulsar_rabbitmq_source) do
    case metadata[:properties] do
      %{"__rabbitmq_consumer_tag" => consumer_tag} -> consumer_tag
      _properties -> nil
    end
  end
end

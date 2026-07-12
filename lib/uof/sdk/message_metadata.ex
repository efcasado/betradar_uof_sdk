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
    partition_key(metadata[:single_metadata]) ||
      partition_key(metadata[:metadata]) ||
      ""
  end

  # Both adapters resolve the token to the AMQP consumer tag of the consume
  # session actually attached to Betradar: the app's own session for AMQP, the
  # RabbitMQ source connector's session for Pulsar. Server-generated tags
  # (`amq.ctag-…`) are unique per consume, so a changed tag is exactly "a
  # delivery gap was possible" — and the token persists as a plain string.
  @spec connection_token(Message.t(), adapter(), atom() | nil) :: term() | nil
  def connection_token(%Message{metadata: metadata}, :amqp, nil), do: Map.get(metadata, :consumer_tag)
  def connection_token(%Message{metadata: metadata}, :amqp, key), do: Map.get(metadata, key)

  def connection_token(%Message{metadata: metadata}, :pulsar_rabbitmq_source, _key) do
    case properties(metadata) do
      %{"__rabbitmq_consumer_tag" => consumer_tag} -> consumer_tag
      _properties -> nil
    end
  end

  defp partition_key(values) when is_list(values) do
    Enum.find_value(values, &partition_key/1)
  end

  defp partition_key(%{partition_key: key}) when is_binary(key), do: key
  defp partition_key(_metadata), do: nil

  defp properties(metadata) do
    metadata
    |> Map.take([:single_metadata, :metadata])
    |> Map.values()
    |> Enum.reduce(%{}, &Map.merge(&2, properties_from_metadata(&1)))
  end

  defp properties_from_metadata(values) when is_list(values) do
    values
    |> Enum.map(&properties_from_metadata/1)
    |> Enum.reduce(%{}, &Map.merge(&2, &1))
  end

  defp properties_from_metadata(%{properties: properties}), do: properties_from_key_values(properties)
  defp properties_from_metadata(_metadata), do: %{}

  defp properties_from_key_values(values) when is_list(values) do
    Map.new(values, fn
      %{key: key, value: value} -> {key, value}
      %{"key" => key, "value" => value} -> {key, value}
      {key, value} -> {to_string(key), value}
    end)
  end

  defp properties_from_key_values(values) when is_map(values),
    do: Map.new(values, fn {key, value} -> {to_string(key), value} end)

  defp properties_from_key_values(_values), do: %{}
end

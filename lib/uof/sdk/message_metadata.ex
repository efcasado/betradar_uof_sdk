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

  @spec connection_token(Message.t(), adapter(), atom() | nil) :: term() | nil
  def connection_token(message, :amqp, nil), do: connection_pid(message)
  def connection_token(%Message{metadata: metadata}, :amqp, key), do: Map.get(metadata, key)

  def connection_token(%Message{metadata: metadata}, :pulsar_rabbitmq_source, _key) do
    case properties(metadata) do
      %{"queueName" => queue_name, "consumerTag" => consumer_tag} ->
        {queue_name, consumer_tag}

      _properties ->
        nil
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

  defp connection_pid(%Message{metadata: %{amqp_channel: %{conn: %{pid: pid}}}}), do: pid
  defp connection_pid(_message), do: nil
end

defmodule UOF.SDK.MessageMetadata do
  @moduledoc false

  # Reads the properties the RabbitMQ source connector sets on every Pulsar
  # message (see the architecture guide's connector contract).

  alias Broadway.Message

  @spec routing_key(Message.t()) :: binary()
  def routing_key(%Message{} = message) do
    case property(message, "__rabbitmq_routing_key") do
      rk when is_binary(rk) -> rk
      nil -> ""
    end
  end

  @spec consumer_tag(Message.t()) :: binary() | nil
  def consumer_tag(%Message{} = message), do: property(message, "__rabbitmq_consumer_tag")

  @doc false
  @spec delivery_log_metadata(Message.t()) :: keyword()
  def delivery_log_metadata(%Message{metadata: metadata}),
    do: [uof_message_id: metadata[:message_id_string], uof_redelivery_count: metadata[:redelivery_count]]

  defp property(%Message{metadata: metadata}, name) do
    case metadata[:properties] do
      %{^name => value} -> value
      _properties -> nil
    end
  end
end

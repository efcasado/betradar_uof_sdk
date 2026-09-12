if Code.ensure_loaded?(BroadwayRabbitMQ.RabbitmqClient) do
  defmodule UOF.SDK.AMQP.Client do
    @moduledoc false
    @behaviour BroadwayRabbitMQ.RabbitmqClient

    alias BroadwayRabbitMQ.AmqpClient

    require Logger

    @impl true
    defdelegate init(opts), to: AmqpClient
    @impl true
    defdelegate ack(channel, delivery_tag), to: AmqpClient
    @impl true
    defdelegate reject(channel, delivery_tag, opts), to: AmqpClient
    @impl true
    defdelegate consume(channel, config), to: AmqpClient
    @impl true
    defdelegate cancel(channel, consumer_tag), to: AmqpClient
    @impl true
    defdelegate close_connection(config, channel), to: AmqpClient

    @impl true
    def setup_channel(config) do
      case AmqpClient.setup_channel(config) do
        {:error, %{__exception__: true} = exception} ->
          # ChannelPool requires exceptions, but BroadwayRabbitMQ 0.8 only
          # backs off for selected atom/tuple errors. Preserve its reconnect
          # loop instead of exhausting supervisor restarts during an outage.
          Logger.warning(Exception.message(exception))
          {:error, :econnrefused}

        result ->
          result
      end
    end
  end
end

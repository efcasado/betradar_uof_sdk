defmodule UOF.SDK.SystemPipeline do
  @moduledoc """
  Broadway pipeline for UOF system messages.

  This pipeline consumes `alive` and `snapshot_complete` messages separately
  from event content. Keeping system traffic isolated lets `ProducerMonitor`
  own producer lifecycle and recovery correlation, while content processing
  remains partitioned by sport-event URN in `UOF.SDK.ContentPipeline`.

  ## Options

    * `:name` (required) - pipeline name.
    * `:producer` - Broadway producer spec for system messages.
    * `:monitor` - module that receives lifecycle side-effects (`alive` and
      `snapshot_complete`). Default `nil`.
    * `:metadata_adapter` - message metadata shape (`:amqp` or
      `:pulsar_rabbitmq_source`; default `:amqp`).
    * `:routing_key_metadata_key` - the `message.metadata` field carrying the
      UOF routing key for AMQP/custom producers (default `:routing_key`).
    * `:connection_token_metadata_key` - the `message.metadata` field carrying a
      per-connection token for reconnect detection (default `nil`, i.e. the
      AMQP connection pid).
  """

  use Broadway

  alias Broadway.Message
  alias UOF.Schemas
  alias UOF.SDK.MessageMetadata
  alias UOF.SDK.RoutingKey

  require Logger

  @doc false
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      type: :supervisor,
      shutdown: :infinity
    }
  end

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    metadata_adapter = Keyword.get(opts, :metadata_adapter, :amqp)
    routing_key_key = Keyword.get(opts, :routing_key_metadata_key, :routing_key)
    connection_token_key = Keyword.get(opts, :connection_token_metadata_key)

    Broadway.start_link(__MODULE__,
      name: name,
      producer: [module: Keyword.fetch!(opts, :producer), concurrency: 1],
      processors: [default: [concurrency: 1]],
      partition_by: &partition(&1, metadata_adapter, routing_key_key),
      context: %{
        monitor: Keyword.get(opts, :monitor),
        metadata_adapter: metadata_adapter,
        routing_key_key: routing_key_key,
        connection_token_key: connection_token_key
      }
    )
  end

  @impl Broadway
  def handle_message(_processor, %Message{} = message, context) do
    rk = message |> routing_key(context) |> RoutingKey.parse()

    if rk.message_type in ["alive", "snapshot_complete"] do
      handle_system_message(message, context, rk)
    else
      message
    end
  end

  defp handle_system_message(%Message{} = message, context, rk) do
    case Schemas.XML.decode(message.data) do
      {:ok, decoded} ->
        maybe_track_connection(context, rk.message_type, message)
        observe(context, rk, decoded)
        message

      {:error, reason} ->
        Message.failed(message, reason)
    end
  end

  @impl Broadway
  def handle_failed(messages, context) do
    for message <- messages do
      rk = message |> routing_key(context) |> RoutingKey.parse()

      :telemetry.execute(
        [:uof_sdk, :message, :failed],
        %{payload_bytes: byte_size(message.data)},
        %{routing_key: rk.raw, message_type: rk.message_type, reason: message.status}
      )

      Logger.error("UOF system message processing failed: #{inspect(message.status)}",
        uof_reason: inspect(message.status),
        uof_routing_key: rk.raw,
        uof_message_type: rk.message_type,
        uof_event_urn: rk.event_urn,
        uof_redelivered: message.metadata[:redelivered],
        uof_delivery_tag: message.metadata[:delivery_tag],
        uof_payload: truncate(message.data, 4096)
      )
    end

    messages
  end

  defp truncate(bin, max) when byte_size(bin) > max, do: binary_part(bin, 0, max)
  defp truncate(bin, _max), do: bin

  ## lifecycle side-effects --------------------------------------------------

  defp observe(ctx, %RoutingKey{message_type: "alive"}, msg) do
    notify(ctx.monitor, :alive, [msg.product, msg.timestamp, msg.subscribed == 1])
  end

  defp observe(ctx, %RoutingKey{message_type: "snapshot_complete"}, msg) do
    notify(ctx.monitor, :snapshot_complete, [msg.product, msg.request_id])
  end

  defp observe(_ctx, %RoutingKey{}, _msg), do: :ok

  defp maybe_track_connection(%{monitor: nil}, _type, _message), do: :ok

  defp maybe_track_connection(%{monitor: monitor, connection_token_key: key} = context, "alive", message) do
    case connection_token(message, %{context | connection_token_key: key}) do
      nil -> :ok
      token -> monitor.observe_connection({:system, token})
    end
  end

  defp maybe_track_connection(_context, _type, _message), do: :ok

  defp connection_token(message, context) do
    MessageMetadata.connection_token(message, context.metadata_adapter, context.connection_token_key)
  end

  defp notify(nil, _fun, _args), do: :ok
  defp notify(mod, fun, args), do: apply(mod, fun, args)

  ## partitioning ------------------------------------------------------------

  defp partition(%Message{} = message, metadata_adapter, routing_key_key) do
    message
    |> routing_key(%{metadata_adapter: metadata_adapter, routing_key_key: routing_key_key})
    |> RoutingKey.parse()
    |> RoutingKey.partition_key()
    |> :erlang.phash2()
  end

  defp routing_key(%Message{} = message, context) do
    MessageMetadata.routing_key(message, context.metadata_adapter, context.routing_key_key)
  end
end

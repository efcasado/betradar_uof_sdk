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
    * `:node_id` - this SDK node's id. `snapshot_complete` addressed to another
      node is acked without reaching the monitor, so a colliding `request_id`
      from another node cannot complete our recovery. `nil` accepts every node.
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

    Broadway.start_link(__MODULE__,
      name: name,
      producer: [module: Keyword.fetch!(opts, :producer), concurrency: 1],
      processors: [default: [concurrency: 1]],
      partition_by: &partition(&1, metadata_adapter, routing_key_key),
      context: %{
        monitor: Keyword.get(opts, :monitor),
        node_id: Keyword.get(opts, :node_id),
        metadata_adapter: metadata_adapter,
        routing_key_key: routing_key_key
      }
    )
  end

  @impl Broadway
  def handle_message(_processor, %Message{} = message, context) do
    rk = message |> routing_key(context) |> RoutingKey.parse()

    cond do
      rk.message_type not in ["alive", "snapshot_complete"] -> message
      RoutingKey.for_node?(rk, context.node_id) -> handle_system_message(message, context, rk)
      true -> skip_other_node(message, rk)
    end
  end

  # Expected when several subscriptions share an access token; a steady rate on
  # a single subscription points at instances configured with mixed node ids.
  defp skip_other_node(%Message{} = message, rk) do
    :telemetry.execute([:uof_sdk, :message, :other_node], %{count: 1}, %{
      routing_key: rk.raw,
      message_type: rk.message_type,
      node_id: rk.node_id
    })

    message
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
    # `subscribed=0` is itself a recovery signal. A malformed/missing timestamp
    # must not hide it; the monitor can observe an alive without checkpointing.
    if msg.product do
      notify(ctx.monitor, :alive, [msg.product, msg.timestamp, msg.subscribed == 1])
    end
  end

  defp observe(ctx, %RoutingKey{message_type: "snapshot_complete"}, msg) do
    notify(ctx.monitor, :snapshot_complete, [msg.product, msg.request_id])
  end

  defp observe(_ctx, %RoutingKey{}, _msg), do: :ok

  defp maybe_track_connection(%{monitor: nil}, _type, _message), do: :ok

  defp maybe_track_connection(%{monitor: monitor} = context, "alive", message) do
    case MessageMetadata.consumer_tag(message, context.metadata_adapter) do
      nil -> :ok
      tag -> monitor.observe_connection({:system, tag})
    end
  end

  defp maybe_track_connection(_context, _type, _message), do: :ok

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

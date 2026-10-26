defmodule UOF.SDK.SystemPipeline do
  @moduledoc """
  Broadway pipeline for UOF system messages.

  This pipeline consumes `alive` and `snapshot_complete` messages separately
  from event content. Keeping system traffic isolated lets `ProducerMonitor`
  own producer lifecycle and recovery correlation, while content processing
  remains partitioned by sport-event URN in `UOF.SDK.ContentPipeline`.

  ## Options

    * `:name` (required) - pipeline name.
    * `:producer` - Broadway producer spec for the Failover system
      subscription. It receives every message on the topic; event content is
      acked without decoding.
    * `:monitor` - module that receives lifecycle side-effects (`alive`, with
      the upstream consumer tag it was delivered in, and `snapshot_complete`).
      Default `nil`.
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
    Broadway.start_link(__MODULE__,
      name: Keyword.fetch!(opts, :name),
      producer: [module: Keyword.fetch!(opts, :producer), concurrency: 1],
      processors: [default: [concurrency: 1]],
      partition_by: &partition/1,
      context: %{monitor: Keyword.get(opts, :monitor), node_id: Keyword.get(opts, :node_id)}
    )
  end

  @impl Broadway
  def handle_message(_processor, %Message{} = message, context) do
    rk = message |> MessageMetadata.routing_key() |> RoutingKey.parse()

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
        observe(context, rk, decoded, message)
        message

      {:error, reason} ->
        Message.failed(message, reason)
    end
  end

  @impl Broadway
  def handle_failed(messages, _context) do
    for message <- messages do
      rk = message |> MessageMetadata.routing_key() |> RoutingKey.parse()

      :telemetry.execute(
        [:uof_sdk, :message, :failed],
        %{payload_bytes: byte_size(message.data)},
        %{routing_key: rk.raw, message_type: rk.message_type, reason: message.status}
      )

      Logger.error(
        "UOF system message processing failed: #{inspect(message.status)}",
        [
          uof_reason: inspect(message.status),
          uof_routing_key: rk.raw,
          uof_message_type: rk.message_type,
          uof_event_urn: rk.event_urn,
          uof_payload: truncate(message.data, 4096)
        ] ++ MessageMetadata.delivery_log_metadata(message)
      )
    end

    messages
  end

  defp truncate(bin, max) when byte_size(bin) > max, do: binary_part(bin, 0, max)
  defp truncate(bin, _max), do: bin

  ## lifecycle side-effects --------------------------------------------------

  # An alive carries the consumer tag it was delivered in, so the monitor
  # checks the upstream session in the same step as the alive itself.
  defp observe(ctx, %RoutingKey{message_type: "alive"}, msg, message) do
    # `subscribed=0` is itself a recovery signal. A malformed/missing timestamp
    # must not hide it; the monitor can observe an alive without checkpointing.
    if msg.product do
      consumer_tag = MessageMetadata.consumer_tag(message)
      notify(ctx.monitor, :alive, [msg.product, msg.timestamp, msg.subscribed == 1, consumer_tag])
    end
  end

  defp observe(ctx, %RoutingKey{message_type: "snapshot_complete"}, msg, _message) do
    notify(ctx.monitor, :snapshot_complete, [msg.product, msg.request_id])
  end

  defp observe(_ctx, %RoutingKey{}, _msg, _message), do: :ok

  defp notify(nil, _fun, _args), do: :ok
  defp notify(mod, fun, args), do: apply(mod, fun, args)

  ## partitioning ------------------------------------------------------------

  defp partition(%Message{} = message) do
    message
    |> MessageMetadata.routing_key()
    |> RoutingKey.parse()
    |> RoutingKey.partition_key()
    |> :erlang.phash2()
  end
end

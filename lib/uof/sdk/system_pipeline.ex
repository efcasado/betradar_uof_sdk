defmodule UOF.SDK.SystemPipeline do
  @moduledoc """
  Broadway pipeline for UOF system messages.

  This pipeline consumes `alive` and `snapshot_complete` messages separately
  from event content. Keeping system traffic isolated lets `ProducerMonitor`
  own producer lifecycle and recovery correlation, while content processing
  remains partitioned by sport-event URN in `UOF.SDK.ContentPipeline`.

  ## Options

    * `:name` (required) - pipeline name.
    * `:handler` (required) - module implementing `UOF.SDK.MessageHandler`.
    * `:connection` - keyword list passed to `BroadwayRabbitMQ.Producer`.
      Ignored when `:producer` is set.
    * `:node_id` - integer identifying this SDK instance on a shared account.
      Used to scope `snapshot_complete` messages to this node.
    * `:producer` - a custom Broadway producer spec for system messages.
    * `:monitor` - module that receives lifecycle side-effects (`alive` and
      `snapshot_complete`). Default `nil`.
    * `:routing_key_metadata_key` - the `message.metadata` field carrying the
      UOF routing key (default `:routing_key`).
    * `:connection_token_metadata_key` - the `message.metadata` field carrying a
      per-connection token for reconnect detection (default `nil`, i.e. the
      AMQP connection pid).
  """

  use Broadway

  alias Broadway.Message
  alias UOF.Schemas
  alias UOF.SDK.Context
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
    handler = Keyword.fetch!(opts, :handler)
    routing_key_key = Keyword.get(opts, :routing_key_metadata_key, :routing_key)
    connection_token_key = Keyword.get(opts, :connection_token_metadata_key)

    Broadway.start_link(__MODULE__,
      name: name,
      producer: [module: build_producer(opts), concurrency: 1],
      processors: [default: [concurrency: 1]],
      partition_by: &partition(&1, routing_key_key),
      context: %{
        handler: handler,
        monitor: Keyword.get(opts, :monitor),
        routing_key_key: routing_key_key,
        connection_token_key: connection_token_key
      }
    )
  end

  @impl Broadway
  def handle_message(_processor, %Message{} = message, context) do
    rk = message |> routing_key(context.routing_key_key) |> RoutingKey.parse()

    case Schemas.XML.decode(message.data) do
      {:ok, decoded} ->
        ctx = %Context{
          producer_id: Map.get(decoded, :product),
          message_type: rk.message_type,
          routing_key: rk.raw,
          event_urn: rk.event_urn
        }

        deliver(context.handler, rk.message_type, decoded, ctx)
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
      rk = message |> routing_key(context.routing_key_key) |> RoutingKey.parse()

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

  defp maybe_track_connection(%{monitor: monitor, connection_token_key: key}, "alive", message) do
    case connection_token(message, key) do
      nil -> :ok
      token -> monitor.observe_connection({:system, token})
    end
  end

  defp maybe_track_connection(_context, _type, _message), do: :ok

  defp connection_token(message, nil), do: connection_pid(message)
  defp connection_token(%Message{metadata: metadata}, key), do: Map.get(metadata, key)

  defp connection_pid(%Message{metadata: %{amqp_channel: %{conn: %{pid: pid}}}}), do: pid
  defp connection_pid(_message), do: nil

  defp notify(nil, _fun, _args), do: :ok
  defp notify(mod, fun, args), do: apply(mod, fun, args)

  ## delivery to the user handler --------------------------------------------

  defp deliver(handler, "alive", msg, ctx), do: handler.handle_alive(msg, ctx)
  defp deliver(_handler, _system_type, _msg, _ctx), do: :ok

  ## partitioning ------------------------------------------------------------

  defp partition(%Message{} = message, routing_key_key) do
    message
    |> routing_key(routing_key_key)
    |> RoutingKey.parse()
    |> RoutingKey.partition_key()
    |> :erlang.phash2()
  end

  defp routing_key(%Message{metadata: metadata}, key) do
    case Map.get(metadata, key) do
      rk when is_binary(rk) -> rk
      _ -> ""
    end
  end

  ## producer ----------------------------------------------------------------

  @exchange "unifiedfeed"

  defp build_producer(opts) do
    case Keyword.get(opts, :producer) do
      nil ->
        {BroadwayRabbitMQ.Producer,
         queue: "",
         connection: Keyword.get(opts, :connection, []),
         declare: [exclusive: true, auto_delete: true],
         bindings: system_bindings(Keyword.get(opts, :node_id)),
         on_failure: :reject,
         metadata: [:routing_key, :redelivered, :delivery_tag]}

      producer ->
        producer
    end
  end

  defp system_bindings(node_id) when is_integer(node_id) and node_id > 0 do
    [
      {@exchange, routing_key: "-.-.-.alive.#"},
      {@exchange, routing_key: "-.-.-.snapshot_complete.*.*.*.#{node_id}.#"}
    ]
  end

  defp system_bindings(_node_id) do
    [
      {@exchange, routing_key: "-.-.-.alive.#"},
      {@exchange, routing_key: "-.-.-.snapshot_complete.#"}
    ]
  end
end

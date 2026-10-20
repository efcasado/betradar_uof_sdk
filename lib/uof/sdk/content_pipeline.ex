defmodule UOF.SDK.ContentPipeline do
  @moduledoc """
  Broadway pipeline that consumes one AMQP scope session (live / prematch /
  virt), decodes each message and dispatches it to the configured
  `UOF.SDK.MessageHandler`.

  Event messages are partitioned across processors by sport-event URN
  (`partition_by`) so that all messages for a given event are handled in order
  by the same processor. System messages are handled by
  `UOF.SDK.SystemPipeline`.

  ## Options

    * `:name` (required) — pipeline name.
    * `:handler` (required) — module implementing `UOF.SDK.MessageHandler`.
    * `:concurrency` — processor concurrency (default `10`).
    * `:producer` — Broadway producer spec for event content.
    * `:monitor` — module that receives content timestamp side-effects.
      Default `nil`, in which case messages are only delivered to the handler.
    * `:metadata_adapter` — message metadata shape (`:amqp` or
      `:pulsar_rabbitmq_source`; default `:amqp`).
    * `:routing_key_metadata_key` — the `message.metadata` field carrying the
      UOF routing key for AMQP/custom producers (default `:routing_key`).
    * `:node_id` — this SDK node's id. Messages addressed to another node
      (recovery replays it requested) are acked without dispatch. `nil`
      accepts every node.

  ## Producer contract

  The SDK derives the producer spec from `UOF.SDK.Config` transport settings.
  The producer must honour this contract:

    1. **Routing key available through the configured metadata adapter**.
       Dispatch, partitioning and content timestamp observation derive from it
       (`UOF.SDK.RoutingKey`); without it nothing routes. AMQP/custom producers
       read a flat `message.metadata[:routing_key]` by default. The built-in
       Pulsar adapter reads the `__rabbitmq_routing_key` property emitted by the
       RabbitMQ source connector.
    2. **Raw UOF XML → `message.data`**, byte-for-byte. `UOF.Schemas.XML.decode/1`
       expects the verbatim feed payload; a connector that wraps or re-encodes
       the body breaks decoding.
    3. **Failure/ack semantics are the producer's own.** The RabbitMQ default
       rejects without requeue (see "Failure handling"); a custom producer
       brings its own equivalent (e.g. a Pulsar DLQ topic + bounded redelivery).

  ## Failure handling

  Processing is **single-attempt**. A message that fails to decode (or whose
  processing crashes) is rejected **without requeue** (`on_failure: :reject`) —
  Betradar's broker has no dead-letter queue we can declare, and requeuing a
  message that deterministically fails to parse would loop forever. Content gaps
  from dropped messages are closed by recovery, not redelivery.

  Every failure passes through `handle_failed/2`, which logs the routing key,
  reason and (truncated) payload — the only forensic record, since there is no
  DLQ — and emits `[:uof_sdk, :message, :failed]` telemetry for alerting.

  Retrying a handler's side-effects (for a transient downstream) is the
  handler's concern: only it knows whether re-applying is idempotent, and
  whether a retried odds message is still fresh enough to matter.
  """

  use Broadway

  alias Broadway.Message
  alias UOF.Schemas
  alias UOF.SDK.Context
  alias UOF.SDK.MessageMetadata
  alias UOF.SDK.RoutingKey

  require Logger

  @content_message_types UOF.SDK.MessageHandler.content_message_types()

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
    concurrency = Keyword.get(opts, :concurrency, 10)
    metadata_adapter = Keyword.get(opts, :metadata_adapter, :amqp)
    routing_key_key = Keyword.get(opts, :routing_key_metadata_key, :routing_key)

    Broadway.start_link(__MODULE__,
      name: name,
      producer: [module: Keyword.fetch!(opts, :producer), concurrency: 1],
      processors: [default: [concurrency: concurrency]],
      # `partition_by` runs in the producer's dispatcher (no context), so the
      # routing-key field is captured in the closure rather than read from context.
      partition_by: &partition(&1, metadata_adapter, routing_key_key),
      context: %{
        handler: handler,
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

    if RoutingKey.for_node?(rk, context.node_id) do
      handle_node_message(message, context, rk)
    else
      skip_other_node(message, rk)
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

  # The AMQP bindings already scope by node; Pulsar's source connector binds
  # `#`, so another node's recovery replay on a shared access token reaches
  # every subscriber and must be dropped here.
  defp handle_node_message(%Message{} = message, context, rk) do
    case Schemas.XML.decode(message.data) do
      {:ok, decoded} ->
        ctx = %Context{
          producer_id: Map.get(decoded, :product),
          message_type: rk.message_type,
          routing_key: rk.raw,
          event_urn: rk.event_urn
        }

        track_processing(context, rk, decoded, fn -> deliver(context.handler, rk.message_type, decoded, ctx) end)
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

      # Structured fields as Logger metadata (namespaced to avoid collisions) so
      # backends can emit JSON; the message stays a short human-readable summary.
      # Rendering (plain vs structured) is the app's Logger config concern.
      Logger.error("UOF message processing failed: #{inspect(message.status)}",
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
    product = Map.get(msg, :product)
    timestamp = Map.get(msg, :timestamp)

    if product && timestamp do
      notify(ctx.monitor, :message, [product, timestamp])
    end
  end

  defp observe(_ctx, %RoutingKey{}, _msg), do: :ok

  # The monitor measures content lag from the oldest message still being
  # handled, so a slow handler on one processor is visible even while other
  # processors complete fresher messages. Completion also records the content
  # timestamp; only content-session alives go through `message/3`.
  defp track_processing(%{monitor: monitor}, %RoutingKey{message_type: type}, msg, fun)
       when not is_nil(monitor) and type in @content_message_types do
    product = Map.get(msg, :product)
    timestamp = Map.get(msg, :timestamp)

    if product && timestamp, do: monitor.track_processing(product, timestamp, fun), else: fun.()
  end

  defp track_processing(_context, _rk, _msg, fun), do: fun.()

  defp maybe_track_connection(%{monitor: nil}, _type, _message), do: :ok

  defp maybe_track_connection(%{metadata_adapter: :amqp} = context, "alive", message) do
    notify_connection(context, message)
  end

  defp maybe_track_connection(%{metadata_adapter: :pulsar_rabbitmq_source} = context, _type, message) do
    notify_connection(context, message)
  end

  defp maybe_track_connection(_context, _type, _message), do: :ok

  defp notify_connection(%{monitor: monitor} = context, message) do
    case MessageMetadata.consumer_tag(message, context.metadata_adapter) do
      nil -> :ok
      tag -> if report_tag?(tag), do: monitor.observe_connection({:content, tag})
    end
  end

  # Pulsar carries the consumer tag on every message. Each processor reports a
  # tag when it changes and otherwise at most once per interval: the repeats
  # matter, because a monitor promoted to active learns the current tags from
  # them, but one per interval is enough. Broadway processors have no state of
  # their own, so the last report is kept in the processor's dictionary.
  @tag_report_interval_ms 1_000

  defp report_tag?(tag) do
    now = System.monotonic_time(:millisecond)

    case Process.get(:uof_sdk_reported_tag) do
      {^tag, reported_at} when now - reported_at < @tag_report_interval_ms ->
        false

      _other ->
        Process.put(:uof_sdk_reported_tag, {tag, now})
        true
    end
  end

  defp notify(nil, _fun, _args), do: :ok
  defp notify(mod, fun, args), do: apply(mod, fun, args)

  ## delivery to the user handler --------------------------------------------

  defp deliver(handler, "odds_change", msg, ctx), do: handler.handle_odds_change(msg, ctx)
  defp deliver(handler, "bet_settlement", msg, ctx), do: handler.handle_bet_settlement(msg, ctx)
  defp deliver(handler, "bet_stop", msg, ctx), do: handler.handle_bet_stop(msg, ctx)
  defp deliver(handler, "bet_cancel", msg, ctx), do: handler.handle_bet_cancel(msg, ctx)

  defp deliver(handler, "rollback_bet_cancel", msg, ctx), do: handler.handle_rollback_bet_cancel(msg, ctx)

  defp deliver(handler, "rollback_bet_settlement", msg, ctx), do: handler.handle_rollback_bet_settlement(msg, ctx)

  defp deliver(handler, "fixture_change", msg, ctx), do: handler.handle_fixture_change(msg, ctx)

  defp deliver(_handler, _unknown_type, _msg, _ctx), do: :ok

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

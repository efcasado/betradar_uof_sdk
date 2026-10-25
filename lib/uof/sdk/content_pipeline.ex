defmodule UOF.SDK.ContentPipeline do
  @moduledoc """
  Broadway pipeline that consumes the Key-Shared content subscription, decodes
  each event message and dispatches it to the configured
  `UOF.SDK.MessageHandler`.

  Event messages are partitioned across processors by sport-event URN
  (`partition_by`) so that all messages for a given event are handled in order
  by the same processor. The content subscription also receives `alive` and
  `snapshot_complete`; they are acked without decoding, since
  `UOF.SDK.SystemPipeline` handles them on the system subscription. This
  pipeline does not report to the producer monitor, because feed continuity is
  observed there.

  ## Options

    * `:name` (required) — pipeline name.
    * `:handler` (required) — module implementing `UOF.SDK.MessageHandler`.
    * `:concurrency` — processor concurrency (default `10`).
    * `:producer` — Broadway producer spec for the content subscription.
    * `:node_id` — this SDK node's id. Messages addressed to another node
      (recovery replays it requested) are acked without dispatch. `nil`
      accepts every node.

  ## Message contract

  The SDK derives the producer spec from `UOF.SDK.Config`. Messages follow the
  RabbitMQ source connector contract:

    1. **Routing key in the `__rabbitmq_routing_key` property.** Dispatch and
       partitioning derive from it (`UOF.SDK.RoutingKey`); without it nothing
       routes.
    2. **Raw UOF XML → `message.data`**, byte-for-byte. `UOF.Schemas.XML.decode/1`
       expects the verbatim feed payload; a connector that wraps or re-encodes
       the body breaks decoding.

  ## Failure handling

  Processing is **single-attempt**: redelivering a message that
  deterministically fails to parse would loop forever. A failed message is
  nacked and, about a second later, diverted to the
  `<topic>-<subscription>-DLQ` topic instead of being processed again (see
  `UOF.SDK.Pulsar`), where it can be inspected.

  Every failure passes through `handle_failed/2`, which logs the routing key,
  reason, delivery details and (truncated) payload, and emits
  `[:uof_sdk, :message, :failed]` telemetry for alerting.

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

    Broadway.start_link(__MODULE__,
      name: name,
      producer: [module: Keyword.fetch!(opts, :producer), concurrency: 1],
      processors: [default: [concurrency: concurrency]],
      partition_by: &partition/1,
      context: %{handler: handler, node_id: Keyword.get(opts, :node_id)}
    )
  end

  @impl Broadway
  def handle_message(_processor, %Message{} = message, context) do
    rk = message |> MessageMetadata.routing_key() |> RoutingKey.parse()

    cond do
      rk.message_type not in @content_message_types -> message
      RoutingKey.for_node?(rk, context.node_id) -> handle_node_message(message, context, rk)
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

  # The source connector binds `#`, so another node's recovery replay on a
  # shared access token reaches every subscriber and must be dropped here.
  defp handle_node_message(%Message{} = message, context, rk) do
    case Schemas.XML.decode(message.data) do
      {:ok, decoded} ->
        ctx = %Context{
          producer_id: Map.get(decoded, :product),
          message_type: rk.message_type,
          routing_key: rk.raw,
          event_urn: rk.event_urn
        }

        deliver(context.handler, rk.message_type, decoded, ctx)
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

      # Structured fields as Logger metadata (namespaced to avoid collisions) so
      # backends can emit JSON; the message stays a short human-readable summary.
      # Rendering (plain vs structured) is the app's Logger config concern.
      Logger.error(
        "UOF message processing failed: #{inspect(message.status)}",
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

  ## delivery to the user handler --------------------------------------------

  defp deliver(handler, "odds_change", msg, ctx), do: handler.handle_odds_change(msg, ctx)
  defp deliver(handler, "bet_settlement", msg, ctx), do: handler.handle_bet_settlement(msg, ctx)
  defp deliver(handler, "bet_stop", msg, ctx), do: handler.handle_bet_stop(msg, ctx)
  defp deliver(handler, "bet_cancel", msg, ctx), do: handler.handle_bet_cancel(msg, ctx)

  defp deliver(handler, "rollback_bet_cancel", msg, ctx), do: handler.handle_rollback_bet_cancel(msg, ctx)

  defp deliver(handler, "rollback_bet_settlement", msg, ctx), do: handler.handle_rollback_bet_settlement(msg, ctx)

  defp deliver(handler, "fixture_change", msg, ctx), do: handler.handle_fixture_change(msg, ctx)

  ## partitioning ------------------------------------------------------------

  defp partition(%Message{} = message) do
    message
    |> MessageMetadata.routing_key()
    |> RoutingKey.parse()
    |> RoutingKey.partition_key()
    |> :erlang.phash2()
  end
end

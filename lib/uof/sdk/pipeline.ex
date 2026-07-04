defmodule UOF.SDK.Pipeline do
  @moduledoc """
  Broadway pipeline that consumes one AMQP scope session (live / prematch /
  virt), decodes each message and dispatches it to the configured
  `UOF.SDK.MessageHandler`.

  Messages are partitioned across processors by sport-event URN
  (`partition_by`) so that all messages for a given event are handled in order
  by the same processor; system messages without an event collapse into a
  single partition.

  ## Options

    * `:name` (required) — pipeline name.
    * `:handler` (required) — module implementing `UOF.SDK.MessageHandler`.
    * `:concurrency` — processor concurrency (default `10`).
    * `:producer` — a Broadway producer spec. Defaults to a
      `BroadwayRabbitMQ.Producer` built from `:queue` / `:connection` /
      `:bindings`. Tests pass `{Broadway.DummyProducer, []}`; see "Custom
      producers" below for rolling your own transport.
    * `:monitor` / `:checkpoint_store` — modules that receive the lifecycle
      side-effects (`alive`, content timestamps, `snapshot_complete`). Default
      `nil`, in which case messages are only delivered to the handler.
    * `:routing_key_metadata_key` — the `message.metadata` field carrying the
      UOF routing key (default `:routing_key`). See "Custom producers".
    * `:connection_token_metadata_key` — the `message.metadata` field carrying a
      per-connection token for reconnect detection (default `nil`, i.e. the
      AMQP connection pid). See "Custom producers".

  ## Custom producers

  The default `BroadwayRabbitMQ.Producer` is what Betradar's docs recommend, but
  any Broadway producer works (e.g. an `off_broadway_*` producer fed by a
  RabbitMQ source connector). Pass it as `:producer` (`{Module, opts}`). A custom
  producer must honour this contract — it's the whole interface:

    1. **Routing key in `message.metadata`** (a binary). Dispatch, partitioning
       and lifecycle observation all derive from it (`UOF.SDK.RoutingKey`);
       without it nothing routes. The original AMQP routing key must survive the
       transport hop. If the transport surfaces it under a different field than
       `:routing_key` (e.g. a Pulsar source connector that maps it to the
       message key), point `:routing_key_metadata_key` at that field.
    2. **Raw UOF XML → `message.data`**, byte-for-byte. `UOF.Schemas.XML.decode/1`
       expects the verbatim feed payload; a connector that wraps or re-encodes
       the body breaks decoding.
    3. **Failure/ack semantics are the producer's own.** The RabbitMQ default
       rejects without requeue (see "Failure handling"); a custom producer
       brings its own equivalent (e.g. a Pulsar DLQ topic + bounded redelivery).

  ### Reconnect detection across transports

  A reconnect produces a message gap that must trigger recovery. The default
  (RabbitMQ) keys off the `amqp_channel.conn.pid` in metadata — a new pid means
  a new connection. A custom transport can restore this by emitting a
  **per-connection-unique** token as a flat metadata field and pointing
  `:connection_token_metadata_key` at it (the monitor dedups by value, so any
  token that changes on reconnect works). The token must actually roll per
  connection — a stable endpoint string won't trip it. Omit it and reconnect
  recovery is forgone, falling back to `alive`-heartbeat gap detection; this
  degrades cleanly (no crash) but is a real capability difference.

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
    concurrency = Keyword.get(opts, :concurrency, 10)
    routing_key_key = Keyword.get(opts, :routing_key_metadata_key, :routing_key)
    connection_token_key = Keyword.get(opts, :connection_token_metadata_key)

    Broadway.start_link(__MODULE__,
      name: name,
      producer: [module: build_producer(opts), concurrency: 1],
      processors: [default: [concurrency: concurrency]],
      # `partition_by` runs in the producer's dispatcher (no context), so the
      # routing-key field is captured in the closure rather than read from context.
      partition_by: &partition(&1, routing_key_key),
      context: %{
        handler: handler,
        monitor: Keyword.get(opts, :monitor),
        checkpoint_store: Keyword.get(opts, :checkpoint_store),
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
        maybe_track_connection(context, rk.message_type, message)

        ctx = %Context{
          producer_id: Map.get(decoded, :product),
          message_type: rk.message_type,
          routing_key: rk.raw,
          event_urn: rk.event_urn
        }

        observe(context, rk.message_type, decoded)
        deliver(context.handler, rk.message_type, decoded, ctx)
        message

      {:error, reason} ->
        Message.failed(message, reason)
    end
  end

  # Single forensic chokepoint for every failed message (decode errors and
  # processor crashes alike), called just before the message is rejected
  # (`on_failure: :reject`, no requeue — see `build_producer/1`). With no
  # dead-letter queue on Betradar's broker, this log line is the only record of
  # the bytes that failed, so it carries the payload (truncated). Always-on
  # telemetry carries the counts so alerting works regardless of log level.
  @impl Broadway
  def handle_failed(messages, context) do
    for message <- messages do
      rk = message |> routing_key(context.routing_key_key) |> RoutingKey.parse()

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

  # The only forensic copy of the failed bytes, so we keep it — but bounded, so
  # a systemic decode break can't flood the log backend.
  defp truncate(bin, max) when byte_size(bin) > max, do: binary_part(bin, 0, max)
  defp truncate(bin, _max), do: bin

  ## lifecycle side-effects --------------------------------------------------

  # alive / snapshot_complete drive the producer lifecycle; content messages
  # advance the processing clock and the recovery checkpoint.
  defp observe(ctx, "alive", msg) do
    notify(ctx.monitor, :alive, [msg.product, msg.timestamp, msg.subscribed == 1])
    checkpoint(ctx, msg.product, msg.timestamp)
  end

  defp observe(ctx, "snapshot_complete", msg) do
    notify(ctx.monitor, :snapshot_complete, [msg.product, msg.request_id])
  end

  defp observe(ctx, _content_type, msg) do
    product = Map.get(msg, :product)
    timestamp = Map.get(msg, :timestamp)

    if product && timestamp do
      notify(ctx.monitor, :message, [product, timestamp])
      checkpoint(ctx, product, timestamp)
    end
  end

  defp checkpoint(%{checkpoint_store: nil}, _product, _timestamp), do: :ok
  defp checkpoint(%{checkpoint_store: store}, product, timestamp), do: store.put(product, timestamp)

  # A reconnect always yields a new connection token; the monitor dedups and
  # recovers the resulting message gap. Checked only on `alive` (broadcast on
  # every connection, ~10s cadence) to avoid per-message overhead. Absent in
  # tests (DummyProducer), so nil.
  defp maybe_track_connection(%{monitor: nil}, _type, _message), do: :ok

  defp maybe_track_connection(%{monitor: monitor, connection_token_key: key}, "alive", message) do
    case connection_token(message, key) do
      nil -> :ok
      token -> monitor.observe_connection(token)
    end
  end

  defp maybe_track_connection(_context, _type, _message), do: :ok

  # Default (RabbitMQ): the AMQP connection pid, which changes on every reconnect.
  # Custom transports set `:connection_token_metadata_key` to a flat metadata
  # field carrying a per-connection-unique token (see "Custom producers").
  defp connection_token(message, nil), do: connection_pid(message)
  defp connection_token(%Message{metadata: metadata}, key), do: Map.get(metadata, key)

  defp connection_pid(%Message{metadata: %{amqp_channel: %{conn: %{pid: pid}}}}), do: pid
  defp connection_pid(_message), do: nil

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

  defp deliver(handler, "alive", msg, ctx), do: handler.handle_alive(msg, ctx)

  defp deliver(_handler, _system_type, _msg, _ctx), do: :ok

  ## partitioning ------------------------------------------------------------

  # Broadway requires an integer; it does `rem(partition.(msg), n_processors)`.
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

  defp build_producer(opts) do
    case Keyword.get(opts, :producer) do
      nil ->
        {BroadwayRabbitMQ.Producer,
         queue: Keyword.get(opts, :queue, ""),
         connection: Keyword.get(opts, :connection, []),
         declare: [exclusive: true, auto_delete: true],
         bindings: Keyword.get(opts, :bindings, []),
         on_failure: :reject,
         metadata: [:routing_key, :redelivered, :delivery_tag]}

      producer ->
        producer
    end
  end
end

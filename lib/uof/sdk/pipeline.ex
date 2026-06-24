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
      `:bindings`. Tests pass `{Broadway.DummyProducer, []}`.
    * `:monitor` / `:recovery` / `:checkpoint_store` — modules that receive the
      lifecycle side-effects (`alive`, content timestamps, `snapshot_complete`,
      `product_down`). Default `nil`, in which case messages are only delivered
      to the handler.
  """

  use Broadway

  alias Broadway.Message
  alias UOF.Schemas
  alias UOF.SDK.{Context, RoutingKey}

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
      producer: [module: build_producer(opts), concurrency: 1],
      processors: [default: [concurrency: concurrency]],
      partition_by: &partition/1,
      context: %{
        handler: handler,
        monitor: Keyword.get(opts, :monitor),
        recovery: Keyword.get(opts, :recovery),
        checkpoint_store: Keyword.get(opts, :checkpoint_store)
      }
    )
  end

  @impl Broadway
  def handle_message(_processor, %Message{} = message, context) do
    rk = message |> routing_key() |> RoutingKey.parse()

    case Schemas.XML.decode(message.data) do
      {:ok, decoded} ->
        maybe_track_connection(context.monitor, rk.message_type, message)

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

  ## lifecycle side-effects --------------------------------------------------

  # alive / snapshot_complete / product_down drive the producer lifecycle;
  # content messages advance the processing clock and the recovery checkpoint.
  defp observe(ctx, "alive", msg) do
    notify(ctx.monitor, :alive, [msg.product, msg.timestamp, msg.subscribed == 1])
    checkpoint(ctx, msg.product, msg.timestamp)
  end

  defp observe(ctx, "snapshot_complete", msg) do
    notify(ctx.recovery, :snapshot_complete, [msg.product, msg.request_id])
  end

  defp observe(ctx, "product_down", msg) do
    notify(ctx.monitor, :product_down, [msg.product])
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

  # A reconnect always yields a new AMQP connection pid; the monitor dedups and
  # recovers the resulting message gap. Checked only on `alive` (broadcast on
  # every connection, ~10s cadence) to avoid per-message overhead. Absent in
  # tests (DummyProducer), so nil.
  defp maybe_track_connection(nil, _type, _message), do: :ok

  defp maybe_track_connection(monitor, "alive", message) do
    case connection_pid(message) do
      nil -> :ok
      pid -> monitor.observe_connection(pid)
    end
  end

  defp maybe_track_connection(_monitor, _type, _message), do: :ok

  defp connection_pid(%Message{metadata: %{amqp_channel: %{conn: %{pid: pid}}}}), do: pid
  defp connection_pid(_message), do: nil

  defp notify(nil, _fun, _args), do: :ok
  defp notify(mod, fun, args), do: apply(mod, fun, args)

  ## delivery to the user handler --------------------------------------------

  defp deliver(handler, "odds_change", msg, ctx), do: handler.handle_odds_change(msg, ctx)
  defp deliver(handler, "bet_settlement", msg, ctx), do: handler.handle_bet_settlement(msg, ctx)
  defp deliver(handler, "bet_stop", msg, ctx), do: handler.handle_bet_stop(msg, ctx)
  defp deliver(handler, "bet_cancel", msg, ctx), do: handler.handle_bet_cancel(msg, ctx)

  defp deliver(handler, "rollback_bet_cancel", msg, ctx),
    do: handler.handle_rollback_bet_cancel(msg, ctx)

  defp deliver(handler, "rollback_bet_settlement", msg, ctx),
    do: handler.handle_rollback_bet_settlement(msg, ctx)

  defp deliver(handler, "fixture_change", msg, ctx),
    do: handler.handle_fixture_change(msg, ctx)

  defp deliver(handler, "alive", msg, ctx), do: handler.handle_alive(msg, ctx)

  defp deliver(_handler, _system_type, _msg, _ctx), do: :ok

  ## partitioning ------------------------------------------------------------

  # Broadway requires an integer; it does `rem(partition.(msg), n_processors)`.
  defp partition(%Message{} = message) do
    message
    |> routing_key()
    |> RoutingKey.parse()
    |> RoutingKey.partition_key()
    |> :erlang.phash2()
  end

  defp routing_key(%Message{metadata: metadata}) do
    case metadata do
      %{routing_key: rk} when is_binary(rk) -> rk
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
         on_failure: :reject_and_requeue,
         metadata: [:routing_key]}

      producer ->
        producer
    end
  end
end

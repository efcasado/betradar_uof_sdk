defmodule UOF.SDK.Transport do
  @moduledoc false

  alias OffBroadway.Pulsar.Producer

  @exchange "unifiedfeed"

  @content_message_types ~w[
    odds_change
    bet_settlement
    bet_stop
    bet_cancel
    rollback_bet_cancel
    rollback_bet_settlement
    fixture_change
  ]

  @type producer_spec :: {module(), keyword()}
  @type ownership :: :always_active | {:failover, :passive}

  @spec producers(term(), integer() | nil) :: %{
          content: producer_spec(),
          system: producer_spec(),
          ownership: ownership(),
          metadata_adapter: :amqp | :pulsar_rabbitmq_source,
          routing_key_metadata_key: atom(),
          connection_token_metadata_key: atom() | nil
        }
  def producers(transport, node_id) do
    case normalize(transport) do
      {:amqp, opts} -> amqp_producers(opts, node_id)
      {:pulsar, opts} -> pulsar_producers(opts)
      other -> raise ArgumentError, "unsupported UOF.SDK transport: #{inspect(other)}"
    end
  end

  defp normalize(nil), do: {:amqp, []}
  defp normalize(:amqp), do: {:amqp, []}
  defp normalize({:amqp, opts}) when is_list(opts), do: {:amqp, opts}
  defp normalize({:pulsar, opts}) when is_list(opts), do: {:pulsar, opts}
  defp normalize(other), do: other

  defp amqp_producers(opts, node_id) do
    ensure_adapter!(BroadwayRabbitMQ.Producer, :broadway_rabbitmq, :amqp)

    connection = Keyword.get(opts, :connection, [])

    %{
      content: rabbitmq_producer(connection, content_bindings(node_id)),
      system: rabbitmq_producer(connection, system_bindings(node_id)),
      ownership: :always_active,
      metadata_adapter: :amqp,
      routing_key_metadata_key: :routing_key,
      connection_token_metadata_key: nil
    }
  end

  # :consumer_tag metadata is the per-consume connection token used for
  # reconnect detection; the amqp library includes it in every basic_deliver
  # even though broadway_rabbitmq doesn't document it.
  defp rabbitmq_producer(connection, bindings) do
    {BroadwayRabbitMQ.Producer,
     queue: "",
     connection: connection,
     declare: [exclusive: true, auto_delete: true],
     bindings: bindings,
     on_failure: :reject,
     metadata: [:routing_key, :redelivered, :delivery_tag, :consumer_tag]}
  end

  defp pulsar_producers(opts) do
    ensure_adapter!(Producer, :off_broadway_pulsar, :pulsar)

    Keyword.fetch!(opts, :topic)
    subscription = Keyword.fetch!(opts, :subscription)
    base_opts = Keyword.drop(opts, [:routing_key_metadata_key, :connection_token_metadata_key])

    # The system subscription is Failover, so the broker elects one instance
    # as its sole receiver. Ownership reports feed ProducerMonitor, which holds
    # control-plane authority (recovery issuance) only while active. The
    # Key_Shared content subscription never emits these reports.
    #
    # The callback is SDK-owned wiring, not a user extension point: a
    # user-supplied value would silently replace the monitor's ownership
    # signal and leave a demoted instance issuing recoveries forever.
    system_opts =
      Keyword.put(
        base_opts,
        :active_state_callback,
        {UOF.SDK.ProducerMonitor, :active_state_change, []}
      )

    %{
      content: pulsar_producer(base_opts, subscription, :content, :Key_Shared),
      system: pulsar_producer(system_opts, subscription, :system, :Failover),
      ownership: {:failover, :passive},
      metadata_adapter: :pulsar_rabbitmq_source,
      routing_key_metadata_key: :routing_key,
      connection_token_metadata_key: nil
    }
  end

  defp pulsar_producer(opts, subscription, suffix, subscription_type) do
    consumer_opts =
      opts
      |> Keyword.get(:consumer_opts, [])
      |> Keyword.put(:subscription_type, subscription_type)

    opts =
      opts
      |> Keyword.put(:subscription, "#{subscription}-#{suffix}")
      |> Keyword.put(:consumer_opts, consumer_opts)

    {Producer, opts}
  end

  defp ensure_adapter!(module, dependency, transport) do
    if !Code.ensure_loaded?(module) do
      raise ArgumentError,
            "transport #{inspect(transport)} requires optional dependency #{inspect(dependency)}"
    end
  end

  defp content_bindings(node_id) when is_integer(node_id) and node_id > 0 do
    [
      {@exchange, routing_key: "-.-.-.alive.#"}
      | Enum.flat_map(@content_message_types, fn type ->
          [
            {@exchange, routing_key: "*.*.*.#{type}.*.*.*.-.#"},
            {@exchange, routing_key: "*.*.*.#{type}.*.*.*.#{node_id}.#"}
          ]
        end)
    ]
  end

  defp content_bindings(_node_id) do
    [
      {@exchange, routing_key: "-.-.-.alive.#"}
      | Enum.flat_map(@content_message_types, fn type ->
          [
            {@exchange, routing_key: "*.*.*.#{type}.*.*.*.-.#"},
            {@exchange, routing_key: "*.*.*.#{type}.*.*.*.#"}
          ]
        end)
    ]
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

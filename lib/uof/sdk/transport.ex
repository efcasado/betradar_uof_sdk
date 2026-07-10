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

  @spec producers(term(), integer() | nil) :: %{
          content: producer_spec(),
          system: producer_spec(),
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
      metadata_adapter: :amqp,
      routing_key_metadata_key: :routing_key,
      connection_token_metadata_key: nil
    }
  end

  defp rabbitmq_producer(connection, bindings) do
    {BroadwayRabbitMQ.Producer,
     queue: "",
     connection: connection,
     declare: [exclusive: true, auto_delete: true],
     bindings: bindings,
     on_failure: :reject,
     metadata: [:routing_key, :redelivered, :delivery_tag]}
  end

  defp pulsar_producers(opts) do
    ensure_adapter!(Producer, :off_broadway_pulsar, :pulsar)

    Keyword.fetch!(opts, :topic)
    subscription = Keyword.fetch!(opts, :subscription)
    base_opts = Keyword.drop(opts, [:routing_key_metadata_key, :connection_token_metadata_key])

    %{
      content: pulsar_producer(base_opts, subscription, :content, :Key_Shared),
      system: pulsar_producer(base_opts, subscription, :system, :Failover),
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

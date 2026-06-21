defmodule UOF.SDK.Session do
  @moduledoc """
  Routing-key bindings for the feed subscription.

  The SDK uses a **single queue subscribed to every producer's messages** on the
  shared `unifiedfeed` topic exchange — producer separation is an application
  concern (`UOF.SDK.ProducerMonitor`), not a broker one, so there is no benefit
  to splitting scopes for correctness. (Per-scope sessions would only add
  queue-level load isolation; that can be reintroduced later if needed.)

  A full routing key is 8 fields plus an optional producer id:

      priority.prematch.live.message_type.sport.urn.event_id.node_id(.producer_id)

  The catch-all base pattern covers the first 7 fields and is expanded with the
  node-id segment + trailing `#` (see `expand/2`), mirroring the official .NET
  SDK. With a `node_id` set, the queue binds only broadcast (`-`) and its own
  node's messages, isolating it from other clients on a shared account.

  `alive` and `product_down` are broadcast to every client. `snapshot_complete`
  is scoped to `node_id` when set so clients don't consume each other's recovery
  completions.
  """

  @exchange "unifiedfeed"
  @all "*.*.*.*.*.*.*"

  @doc """
  The `BroadwayRabbitMQ.Producer` `:bindings` for the feed queue, scoped to
  `node_id` (a positive integer, or `nil` for none).
  """
  @spec bindings(pos_integer() | nil) :: [{String.t(), [routing_key: String.t()]}]
  def bindings(node_id \\ nil) do
    for routing_key <- expand(@all, node_id) ++ system_keys(node_id) do
      {@exchange, routing_key: routing_key}
    end
  end

  # Append the node-id field (8th) + trailing `#` (optional producer id). With a
  # node id set we bind only broadcast (`-`) and our own node; otherwise any.
  defp expand(pattern, node_id) when is_integer(node_id) and node_id > 0,
    do: ["#{pattern}.-.#", "#{pattern}.#{node_id}.#"]

  defp expand(pattern, _node_id),
    do: ["#{pattern}.-.#", "#{pattern}.#"]

  defp system_keys(node_id) do
    ["-.-.-.alive.#", "-.-.-.product_down.#", snapshot_complete_key(node_id)]
  end

  defp snapshot_complete_key(node_id) when is_integer(node_id) and node_id > 0,
    do: "-.-.-.snapshot_complete.-.-.-.#{node_id}"

  defp snapshot_complete_key(_node_id), do: "-.-.-.snapshot_complete.#"
end

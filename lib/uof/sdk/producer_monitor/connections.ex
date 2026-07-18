defmodule UOF.SDK.ProducerMonitor.Connections do
  @moduledoc false

  # Order-independent consume-session tracking.
  #
  # Observations arrive from concurrent pipeline processors, so delivery order
  # proves nothing. Tokens are unique per consume session (transport contract),
  # which makes novelty the only reliable reconnect signal: a token already in
  # the seen set is a repeat or an out-of-order straggler from a superseded
  # session and must not move the committed baseline backwards. The seen set
  # is seeded with the persisted baseline so resuming into an unchanged
  # session is not a change.
  #
  # Persisted tokens are comparison baselines only — they never arm the
  # startup gate (`active?`); only current-session observations do.

  @required_namespaces [:system, :content]

  defstruct persisted: %{}, observed: %{}, seen: %{}, pending: %{}

  @type t :: %__MODULE__{
          persisted: %{optional(atom()) => term()},
          observed: %{optional(atom()) => term()},
          seen: %{optional(atom()) => MapSet.t()},
          pending: %{optional(atom()) => term()}
        }

  def new(persisted) do
    %__MODULE__{
      persisted: persisted,
      seen: Map.new(persisted, fn {namespace, token} -> {namespace, MapSet.new([token])} end)
    }
  end

  def observe(%__MODULE__{} = connections, namespace, token) do
    connections = track(connections, namespace, token)

    cond do
      not ready?(connections) -> {:not_ready, connections}
      changed?(connections) -> {:recovery_needed, connections}
      true -> {:unchanged, connections}
    end
  end

  defp track(%__MODULE__{} = connections, namespace, token) do
    seen = Map.get(connections.seen, namespace, MapSet.new())

    if MapSet.member?(seen, token) do
      %{connections | observed: Map.put_new(connections.observed, namespace, token)}
    else
      %{
        connections
        | observed: Map.put(connections.observed, namespace, token),
          seen: Map.put(connections.seen, namespace, MapSet.put(seen, token)),
          pending: Map.put(connections.pending, namespace, token)
      }
    end
  end

  def active?(%__MODULE__{} = connections) do
    map_size(connections.observed) > 0
  end

  def ready?(%__MODULE__{observed: observed}) do
    Enum.all?(@required_namespaces, &Map.has_key?(observed, &1))
  end

  defp changed?(%__MODULE__{} = connections) do
    map_size(connections.pending) > 0
  end

  def commit(%__MODULE__{} = connections) do
    %{
      connections
      | persisted: Map.merge(connections.persisted, connections.pending),
        pending: %{}
    }
  end
end

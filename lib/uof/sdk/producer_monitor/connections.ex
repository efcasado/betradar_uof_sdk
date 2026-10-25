defmodule UOF.SDK.ProducerMonitor.Connections do
  @moduledoc false

  # Order-independent consume-session tracking.
  #
  # Observations arrive from concurrent pipeline processors, so delivery order
  # proves nothing. Consumer tags are unique per consume session (transport contract),
  # which makes novelty the only reliable reconnect signal: a tag already in
  # the seen set is a repeat or an out-of-order straggler from a superseded
  # session and must not move the committed baseline backwards. The seen set
  # is seeded with the persisted baseline so resuming into an unchanged
  # session is not a change.
  #
  # Persisted consumer tags are comparison baselines, not readiness evidence. They do
  # arm the restart gate, which remains closed until every required namespace
  # has been observed in this process.
  #
  # The required namespaces are the pipelines the transport monitors. A
  # persisted tag outside them is dropped, so the next commit removes it.

  defstruct required: [], persisted: %{}, observed: %{}, seen: %{}, pending: %{}

  @type t :: %__MODULE__{
          required: [atom()],
          persisted: %{optional(atom()) => term()},
          observed: %{optional(atom()) => term()},
          seen: %{optional(atom()) => MapSet.t()},
          pending: %{optional(atom()) => term()}
        }

  def new(persisted, required) do
    persisted = Map.take(persisted, required)

    %__MODULE__{
      required: required,
      persisted: persisted,
      seen: Map.new(persisted, fn {namespace, tag} -> {namespace, MapSet.new([tag])} end)
    }
  end

  def observe(%__MODULE__{} = connections, namespace, tag) do
    connections = track(connections, namespace, tag)

    cond do
      not ready?(connections) -> {:not_ready, connections}
      changed?(connections) -> {:recovery_needed, connections}
      true -> {:unchanged, connections}
    end
  end

  defp track(%__MODULE__{} = connections, namespace, tag) do
    seen = Map.get(connections.seen, namespace, MapSet.new())

    if MapSet.member?(seen, tag) do
      %{connections | observed: Map.put_new(connections.observed, namespace, tag)}
    else
      %{
        connections
        | observed: Map.put(connections.observed, namespace, tag),
          seen: Map.put(connections.seen, namespace, MapSet.put(seen, tag)),
          pending: Map.put(connections.pending, namespace, tag)
      }
    end
  end

  def active?(%__MODULE__{} = connections) do
    map_size(connections.observed) > 0
  end

  # Persisted consumer tags mean this process is restarting a previously observed
  # session. Once any current tag arrives, an otherwise fresh process is
  # likewise committed to completing the startup gate.
  def awaiting_readiness?(%__MODULE__{} = connections) do
    not ready?(connections) and
      (map_size(connections.persisted) > 0 or active?(connections))
  end

  def missing_namespaces(%__MODULE__{required: required, observed: observed}) do
    Enum.reject(required, &Map.has_key?(observed, &1))
  end

  def ready?(%__MODULE__{required: required, observed: observed}) do
    Enum.all?(required, &Map.has_key?(observed, &1))
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

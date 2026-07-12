defmodule UOF.SDK.ProducerMonitor.Connections do
  @moduledoc false

  @required_namespaces [:system, :content]

  defstruct persisted: %{}, observed: %{}

  @type t :: %__MODULE__{
          persisted: %{optional(atom()) => term()},
          observed: %{optional(atom()) => term()}
        }

  def new(persisted), do: %__MODULE__{persisted: persisted}

  def observe(%__MODULE__{} = connections, namespace, token) do
    %{connections | observed: Map.put(connections.observed, namespace, token)}
  end

  def active?(%__MODULE__{} = connections) do
    map_size(connections.persisted) > 0 or map_size(connections.observed) > 0
  end

  def ready?(%__MODULE__{observed: observed}) do
    Enum.all?(@required_namespaces, &Map.has_key?(observed, &1))
  end

  def changed?(%__MODULE__{} = connections) do
    Enum.any?(connections.observed, fn {namespace, token} ->
      Map.get(connections.persisted, namespace) != token
    end)
  end

  def commit(%__MODULE__{} = connections) do
    %{connections | persisted: Map.merge(connections.persisted, connections.observed)}
  end
end

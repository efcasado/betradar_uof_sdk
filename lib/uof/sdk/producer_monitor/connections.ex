defmodule UOF.SDK.ProducerMonitor.Connections do
  @moduledoc false

  # Order-independent tracking of the upstream consume session.
  #
  # The system pipeline reports the connector's consumer tag from every alive.
  # Consumer tags are unique per consume session (connector contract), which
  # makes novelty the only reliable reconnect signal: a tag already in the
  # seen set is a repeat or an out-of-order straggler from a superseded session
  # and must not move the committed baseline backwards. The seen set is seeded
  # with the persisted baseline so resuming into an unchanged session is not a
  # change.
  #
  # The persisted tag is a comparison baseline, not readiness evidence. It does
  # arm the restart gate, which remains closed until a tag has been observed in
  # this process.

  defstruct persisted: nil, observed: nil, seen: MapSet.new(), pending: nil

  @type t :: %__MODULE__{
          persisted: binary() | nil,
          observed: binary() | nil,
          seen: MapSet.t(),
          pending: binary() | nil
        }

  def new(persisted) do
    %__MODULE__{persisted: persisted, seen: MapSet.new(List.wrap(persisted))}
  end

  def observe(%__MODULE__{} = connections, tag) do
    if MapSet.member?(connections.seen, tag) do
      {:unchanged, %{connections | observed: connections.observed || tag}}
    else
      {:recovery_needed, %{connections | observed: tag, seen: MapSet.put(connections.seen, tag), pending: tag}}
    end
  end

  # A persisted tag means this process is restarting a previously observed
  # session, so recovery waits until the current tag is observed.
  def awaiting_readiness?(%__MODULE__{} = connections) do
    not ready?(connections) and not is_nil(connections.persisted)
  end

  def ready?(%__MODULE__{observed: observed}), do: not is_nil(observed)

  def commit(%__MODULE__{} = connections) do
    %{connections | persisted: connections.pending, pending: nil}
  end
end

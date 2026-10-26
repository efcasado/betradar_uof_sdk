defmodule UOF.SDK.ProducerMonitor.Session do
  @moduledoc false

  # Order-independent tracking of the upstream consume session.
  #
  # Every alive carries the connector's consumer tag. Consumer tags are unique
  # per consume session (connector contract), which makes novelty the only
  # reliable reconnect signal: a tag already in the seen set is a repeat or an
  # out-of-order straggler from a superseded session and must not move the
  # committed tag backwards. The seen set is seeded with the persisted tag so
  # resuming into an unchanged session is not a change.

  defstruct persisted: nil, seen: MapSet.new()

  @type t :: %__MODULE__{persisted: binary() | nil, seen: MapSet.t()}

  def new(persisted) do
    %__MODULE__{persisted: persisted, seen: MapSet.new(List.wrap(persisted))}
  end

  # A tag not seen before is a new session, and becomes the committed one.
  def observe(%__MODULE__{} = session, tag) do
    if MapSet.member?(session.seen, tag) do
      :unchanged
    else
      {:changed, %{session | persisted: tag, seen: MapSet.put(session.seen, tag)}}
    end
  end
end

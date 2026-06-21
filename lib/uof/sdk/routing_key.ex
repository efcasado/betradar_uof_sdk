defmodule UOF.SDK.RoutingKey do
  @moduledoc """
  Parser for Betradar UOF AMQP routing keys.

  Canonical (dot-separated) layout:

      <priority>.<prematch>.<live|virt>.<message_type>.<sport_id>.<urn_type>.<event_id>.<node_id>

  e.g. `hi.-.live.odds_change.1.sr:match.12345.-`, where `-` means "not set".

  The trailing fields vary across producers and system messages (`alive`,
  `snapshot_complete`) carry no sport event at all, so parsing is deliberately
  tolerant: the message type is anchored at index 3, the sport id at index 4,
  and the sport-event URN is located by its `prefix:type` segment (the only
  field containing a `:`) rather than a fixed index.
  """

  @type t :: %__MODULE__{
          raw: String.t(),
          priority: String.t() | nil,
          scope: String.t() | nil,
          message_type: String.t() | nil,
          sport_id: integer() | nil,
          urn_type: String.t() | nil,
          event_id: String.t() | nil,
          event_urn: String.t() | nil,
          node_id: String.t() | nil
        }

  defstruct [
    :raw,
    :priority,
    :scope,
    :message_type,
    :sport_id,
    :urn_type,
    :event_id,
    :event_urn,
    :node_id
  ]

  @doc "Parse a routing key string into a `t:t/0`."
  @spec parse(String.t()) :: t()
  def parse(routing_key) when is_binary(routing_key) do
    parts = String.split(routing_key, ".")
    {urn_type, event_id, event_urn} = event(parts)

    %__MODULE__{
      raw: routing_key,
      priority: parts |> Enum.at(0) |> dash(),
      scope: parts |> Enum.at(2) |> dash(),
      message_type: parts |> Enum.at(3) |> dash(),
      sport_id: parts |> Enum.at(4) |> dash() |> to_int(),
      urn_type: urn_type,
      event_id: event_id,
      event_urn: event_urn,
      node_id: parts |> List.last() |> dash()
    }
  end

  @doc """
  Stable partition key for a parsed routing key: the sport-event URN, or
  `:system` for messages without an event (so all `alive`/`snapshot_complete`
  collapse into a single partition).
  """
  @spec partition_key(t()) :: String.t() | :system
  def partition_key(%__MODULE__{event_urn: nil}), do: :system
  def partition_key(%__MODULE__{event_urn: urn}), do: urn

  defp event(parts) do
    case Enum.find_index(parts, &String.contains?(&1, ":")) do
      nil ->
        {nil, nil, nil}

      i ->
        urn_type = Enum.at(parts, i)
        event_id = parts |> Enum.at(i + 1) |> dash()
        urn = if event_id, do: "#{urn_type}:#{event_id}", else: nil
        {urn_type, event_id, urn}
    end
  end

  defp dash(nil), do: nil
  defp dash("-"), do: nil
  defp dash(""), do: nil
  defp dash(value), do: value

  defp to_int(nil), do: nil
  defp to_int(value), do: String.to_integer(value)
end

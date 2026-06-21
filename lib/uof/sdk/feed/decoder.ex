defmodule UOF.SDK.Feed.Decoder do
  @moduledoc """
  Decode a feed XML message into the matching `UOF.SDK.Feed.*` struct.

  Unlike the HTTP API (where the caller knows the response schema), the AMQP
  feed delivers heterogeneous messages, so decoding first dispatches on the
  root element name and then runs a generic, schema-driven walk.

  The walk is a near-verbatim duplicate of `UOF.API.XML` (which is internal to
  `uof_api`); it is expected to move to the shared codegen package alongside
  the schema generator.
  """

  alias UOF.SDK.Feed

  # Root element name -> generated schema module.
  @roots %{
    "odds_change" => Feed.OddsChange,
    "bet_settlement" => Feed.BetSettlement,
    "bet_stop" => Feed.BetStop,
    "bet_cancel" => Feed.BetCancel,
    "rollback_bet_cancel" => Feed.RollbackBetCancel,
    "rollback_bet_settlement" => Feed.RollbackBetSettlement,
    "fixture_change" => Feed.FixtureChange,
    "alive" => Feed.Alive,
    "snapshot_complete" => Feed.SnapshotComplete,
    "product_down" => Feed.ProductDown
  }

  @doc """
  Decode `xml` into the struct for its root element.

  Returns `{:ok, struct}`, `{:error, %Ecto.Changeset{}}` on a cast failure, or
  `{:error, {:unknown_message, name}}` for an unrecognised root element.
  """
  @spec decode(binary()) ::
          {:ok, struct()}
          | {:error, Ecto.Changeset.t()}
          | {:error, {:unknown_message, String.t()}}
  def decode(xml) when is_binary(xml) do
    {:ok, {tag, _attrs, _children} = root} = Saxy.SimpleForm.parse_string(xml)

    case Map.fetch(@roots, local(tag)) do
      {:ok, module} ->
        module
        |> struct()
        |> module.changeset(to_params(root, module))
        |> Ecto.Changeset.apply_action(:insert)

      :error ->
        {:error, {:unknown_message, local(tag)}}
    end
  end

  @doc "The message type names this decoder understands."
  @spec message_types() :: [String.t()]
  def message_types, do: Map.keys(@roots)

  ## schema-driven walk (duplicated from UOF.API.XML) ------------------------

  defp to_params({_tag, attributes, children}, module) do
    child_elements = Enum.filter(children, &is_tuple/1)
    embeds = module.__schema__(:embeds)
    scalar_fields = module.__schema__(:fields) -- embeds

    attributes
    |> Map.new(fn {name, value} -> {local(name), value} end)
    |> put_scalar_elements(scalar_fields, child_elements)
    |> put_embeds(embeds, child_elements, module)
  end

  defp put_scalar_elements(params, scalar_fields, child_elements) do
    Enum.reduce(scalar_fields, params, fn field, acc ->
      name = Atom.to_string(field)

      cond do
        Map.has_key?(acc, name) -> acc
        element = find(child_elements, name) -> Map.put(acc, name, text(element))
        true -> acc
      end
    end)
  end

  defp put_embeds(params, embeds, child_elements, module) do
    Enum.reduce(embeds, params, fn embed, acc ->
      %Ecto.Embedded{related: related, cardinality: cardinality} =
        module.__schema__(:embed, embed)

      name = Atom.to_string(embed)
      matches = filter(child_elements, name)

      value =
        case cardinality do
          :one -> matches |> List.first() |> maybe_to_params(related)
          :many -> Enum.map(matches, &to_params(&1, related))
        end

      if value in [nil, []], do: acc, else: Map.put(acc, name, value)
    end)
  end

  defp maybe_to_params(nil, _module), do: nil
  defp maybe_to_params(element, module), do: to_params(element, module)

  defp find(elements, name), do: Enum.find(elements, &named?(&1, name))
  defp filter(elements, name), do: Enum.filter(elements, &named?(&1, name))
  defp named?({tag, _a, _c}, name), do: local(tag) == name

  defp text({_tag, _attrs, children}) do
    children |> Enum.filter(&is_binary/1) |> Enum.join() |> String.trim()
  end

  defp local(name) do
    case String.split(name, ":", parts: 2) do
      [_prefix, local] -> local
      [local] -> local
    end
  end
end

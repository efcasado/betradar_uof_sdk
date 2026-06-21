defmodule UOF.SDK.Feed.BetSettlementMarket do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field(:void_reason, :integer)
    field(:result, :string)
    field(:id, :integer)
    field(:specifiers, :string)
    field(:extended_specifiers, :string)
    embeds_many(:outcome, UOF.SDK.Feed.BetSettlementMarketOutcome)
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:void_reason, :result, :id, :specifiers, :extended_specifiers])
    |> cast_embed(:outcome)
  end
end

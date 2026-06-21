defmodule UOF.SDK.Feed.BetSettlementOutcomes do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    embeds_many(:market, UOF.SDK.Feed.BetSettlementMarket)
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [])
    |> cast_embed(:market)
  end
end

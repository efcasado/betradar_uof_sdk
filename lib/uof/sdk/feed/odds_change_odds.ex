defmodule UOF.SDK.Feed.OddsChangeOdds do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field(:betting_status, :integer)
    field(:betstop_reason, :integer)
    embeds_many(:market, UOF.SDK.Feed.OddsChangeMarket)
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:betting_status, :betstop_reason])
    |> cast_embed(:market)
  end
end

defmodule UOF.SDK.Feed.BetSettlement do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field(:certainty, :integer)
    field(:product, :integer)
    field(:event_id, :string)
    field(:timestamp, :integer)
    field(:request_id, :integer)
    embeds_one(:outcomes, UOF.SDK.Feed.BetSettlementOutcomes)
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:certainty, :product, :event_id, :timestamp, :request_id])
    |> cast_embed(:outcomes)
  end
end

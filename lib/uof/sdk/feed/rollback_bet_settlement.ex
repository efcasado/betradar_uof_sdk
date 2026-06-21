defmodule UOF.SDK.Feed.RollbackBetSettlement do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field(:product, :integer)
    field(:event_id, :string)
    field(:timestamp, :integer)
    field(:request_id, :integer)
    embeds_many(:market, UOF.SDK.Feed.Market)
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:product, :event_id, :timestamp, :request_id])
    |> cast_embed(:market)
  end
end

defmodule UOF.SDK.Feed.OddsChange do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field(:odds_change_reason, :integer)
    field(:product, :integer)
    field(:event_id, :string)
    field(:timestamp, :integer)
    field(:request_id, :integer)
    embeds_one(:sport_event_status, UOF.SDK.Feed.SportEventStatus)
    embeds_one(:odds_generation_properties, UOF.SDK.Feed.OddsGenerationProperties)
    embeds_one(:odds, UOF.SDK.Feed.OddsChangeOdds)
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:odds_change_reason, :product, :event_id, :timestamp, :request_id])
    |> cast_embed(:sport_event_status)
    |> cast_embed(:odds_generation_properties)
    |> cast_embed(:odds)
  end
end

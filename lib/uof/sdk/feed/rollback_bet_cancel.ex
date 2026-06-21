defmodule UOF.SDK.Feed.RollbackBetCancel do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field(:start_time, :integer)
    field(:end_time, :integer)
    field(:product, :integer)
    field(:event_id, :string)
    field(:timestamp, :integer)
    field(:request_id, :integer)
    embeds_many(:market, UOF.SDK.Feed.Market)
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:start_time, :end_time, :product, :event_id, :timestamp, :request_id])
    |> cast_embed(:market)
  end
end

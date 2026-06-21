defmodule UOF.SDK.Feed.BetStop do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field(:groups, :string)
    field(:market_status, :integer)
    field(:product, :integer)
    field(:event_id, :string)
    field(:timestamp, :integer)
    field(:request_id, :integer)
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:groups, :market_status, :product, :event_id, :timestamp, :request_id])
  end
end

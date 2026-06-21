defmodule UOF.SDK.Feed.BetSettlementMarketOutcome do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field(:id, :string)
    field(:result, :integer)
    field(:void_factor, :float)
    field(:dead_heat_factor, :float)
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:id, :result, :void_factor, :dead_heat_factor])
  end
end

defmodule UOF.SDK.Feed.Alive do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field(:product, :integer)
    field(:timestamp, :integer)
    field(:subscribed, :integer)
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:product, :timestamp, :subscribed])
  end
end

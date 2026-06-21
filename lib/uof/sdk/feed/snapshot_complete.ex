defmodule UOF.SDK.Feed.SnapshotComplete do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field(:request_id, :integer)
    field(:product, :integer)
    field(:timestamp, :integer)
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:request_id, :product, :timestamp])
  end
end

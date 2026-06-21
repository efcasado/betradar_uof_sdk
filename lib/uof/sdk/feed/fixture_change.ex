defmodule UOF.SDK.Feed.FixtureChange do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field(:change_type, :integer)
    field(:start_time, :integer)
    field(:next_live_time, :integer)
    field(:product, :integer)
    field(:event_id, :string)
    field(:timestamp, :integer)
    field(:request_id, :integer)
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [
      :change_type,
      :start_time,
      :next_live_time,
      :product,
      :event_id,
      :timestamp,
      :request_id
    ])
  end
end

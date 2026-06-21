defmodule UOF.SDK.Feed.OddsGenerationProperties do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field(:expected_totals, :float)
    field(:expected_supremacy, :float)
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:expected_totals, :expected_supremacy])
  end
end

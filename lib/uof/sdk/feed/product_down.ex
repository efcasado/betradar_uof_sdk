defmodule UOF.SDK.Feed.ProductDown do
  @moduledoc """
  `product_down` feed message — Betradar signalling that a producer has stopped
  producing.

  Hand-written rather than generated: `product_down` is not defined in
  `UnifiedFeed.xsd` (it is delivered over the feed but absent from the schema
  set). Shape mirrors the other system messages (`product`, `timestamp`).
  """

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field(:product, :integer)
    field(:timestamp, :integer)
  end

  def changeset(struct, params) do
    cast(struct, params, [:product, :timestamp])
  end
end

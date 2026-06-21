defmodule UOF.SDK.Producers do
  @moduledoc """
  Builds the initial `UOF.SDK.Producer` list the monitor tracks, from Betradar's
  producer descriptions (`UOF.API.Descriptions.producers/0`).

  The recovery `product` path segment is taken from the description's `api_url`
  (e.g. `.../v1/pre` -> `"pre"`), and the recovery window from
  `stateful_recovery_window_in_minutes`.
  """

  require Logger

  alias UOF.SDK.Producer

  @doc """
  Fetch producer descriptions from the API and build the tracked list. Logs and
  returns `[]` on failure so a transient API issue doesn't stop the SDK from
  starting (the monitor seeds nothing and recovers once descriptions are
  available on a later run).
  """
  @spec fetch() :: [Producer.t()]
  def fetch do
    case UOF.API.Descriptions.producers() do
      {:ok, %{producer: descriptions}} when is_list(descriptions) ->
        build(descriptions)

      other ->
        Logger.warning("UOF.SDK.Producers: could not load producers: #{inspect(other)}")
        []
    end
  rescue
    exception ->
      Logger.warning("UOF.SDK.Producers: error loading producers: #{Exception.message(exception)}")
      []
  end

  @doc "Build `UOF.SDK.Producer` structs from a list of API producer descriptions."
  @spec build([map()]) :: [Producer.t()]
  def build(descriptions) do
    descriptions
    |> Enum.filter(& &1.active)
    |> Enum.map(&to_producer/1)
  end

  defp to_producer(description) do
    %Producer{
      id: description.id,
      name: description.name,
      product: product_segment(description.api_url),
      recovery_window_minutes: description.stateful_recovery_window_in_minutes
    }
  end

  defp product_segment(nil), do: nil

  defp product_segment(api_url) do
    api_url |> to_string() |> String.trim_trailing("/") |> String.split("/") |> List.last()
  end
end

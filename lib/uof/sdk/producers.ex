defmodule UOF.SDK.Producers do
  @moduledoc """
  Builds the initial `UOF.SDK.ProducerMonitor.Producer` list the monitor tracks, from Betradar's
  producer descriptions (`UOF.API.Descriptions.producers/0`).

  The recovery `product` path segment is taken from the description's `api_url`
  (e.g. `.../v1/pre` -> `"pre"`), and the recovery window from
  `stateful_recovery_window_in_minutes`.
  """

  alias UOF.SDK.ProducerMonitor.Producer

  @doc """
  Fetch producer descriptions from the API and build the tracked list.

  Raises on failure. Starting the SDK without producer descriptions would leave
  the monitor with no known producers, which disables health tracking and
  recovery until the VM is restarted.
  """
  @spec fetch() :: [Producer.t()]
  def fetch do
    fetch(UOF.API.Descriptions.producers())
  rescue
    exception ->
      raise RuntimeError, "could not load UOF producers: #{Exception.message(exception)}"
  end

  @doc false
  @spec fetch(term()) :: [Producer.t()]
  def fetch({:ok, %{producer: descriptions}}) when is_list(descriptions), do: build(descriptions)

  def fetch(other) do
    raise RuntimeError, "could not load UOF producers: #{inspect(other)}"
  end

  @doc "Build `UOF.SDK.ProducerMonitor.Producer` structs from a list of API producer descriptions."
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

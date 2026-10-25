defmodule UOF.SDK.ProducerMonitor.Descriptions do
  @moduledoc false

  # Loads the producer descriptions recovery needs: each producer's API product
  # and stateful recovery window. The UOF API is authoritative. When it fails,
  # the descriptions stored by a previous run, or else the configured ones, let
  # the monitor start while it keeps retrying the API. With no source at all,
  # startup fails: running without known producers would silently disable
  # health tracking and recovery.

  require Logger

  @type t :: %{
          id: integer(),
          name: String.t() | nil,
          product: String.t() | nil,
          recovery_window_minutes: integer() | nil
        }

  @doc false
  @spec load(keyword(), [t()]) :: {[t()], :api | :snapshot | :config}
  def load(opts, stored) do
    case fetch(opts) do
      {:ok, descriptions} ->
        emit_loaded(descriptions, :api)
        {descriptions, :api}

      {:error, reason} ->
        fall_back(reason, stored, Keyword.get(opts, :producers, []))
    end
  end

  @doc false
  @spec fetch(keyword()) :: {:ok, [t()]} | {:error, term()}
  def fetch(opts) do
    fetcher = Keyword.get(opts, :producer_fetcher, &UOF.API.Descriptions.producers/0)

    case fetcher.() do
      {:ok, %{producer: descriptions}} when is_list(descriptions) ->
        {:ok, descriptions |> Enum.filter(& &1.active) |> Enum.map(&description/1)}

      {:error, reason} ->
        {:error, reason}

      other ->
        {:error, {:unexpected_response, other}}
    end
  end

  @doc false
  @spec emit_loaded([t()], :api | :snapshot | :config) :: :ok
  def emit_loaded(descriptions, source) do
    :telemetry.execute([:uof_sdk, :producers, :loaded], %{count: length(descriptions)}, %{source: source})
  end

  defp fall_back(reason, [_ | _] = stored, _configured), do: degraded(reason, stored, :snapshot)
  defp fall_back(reason, [], [_ | _] = configured), do: degraded(reason, configured, :config)

  defp fall_back(reason, [], []) do
    raise RuntimeError,
          "could not load UOF producers: #{inspect(reason)}; no stored or configured producer descriptions"
  end

  defp degraded(reason, descriptions, source) do
    Logger.warning(
      "UOF.SDK.ProducerMonitor: could not load producer descriptions from the UOF API (#{inspect(reason)}); " <>
        "starting from #{source} descriptions and retrying"
    )

    emit_loaded(descriptions, source)
    {descriptions, source}
  end

  defp description(api_description) do
    %{
      id: api_description.id,
      name: api_description.name,
      product: product_segment(api_description.api_url),
      recovery_window_minutes: api_description.stateful_recovery_window_in_minutes
    }
  end

  defp product_segment(nil), do: nil

  defp product_segment(api_url) do
    api_url |> to_string() |> String.trim_trailing("/") |> String.split("/") |> List.last()
  end
end

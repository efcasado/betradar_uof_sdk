defmodule Mix.Tasks.Uof.Sdk.Xsd.Fetch do
  @moduledoc """
  Download the Betradar XSD files into the git-ignored `priv/xsd/` cache,
  pinned to the upstream SDK tag in `Mix.UOF.SDK.XSD.Sources`.

      mix uof.sdk.xsd.fetch              # all groups
      mix uof.sdk.xsd.fetch custombet    # a single group

  This is a convenience for inspecting/refreshing the schemas; `mix
  uof.sdk.gen.schemas` fetches what it needs on its own.
  """
  use Mix.Task

  @shortdoc "Download Betradar XSDs (pinned to the SDK tag)"

  @impl Mix.Task
  def run(args) do
    known = Mix.UOF.SDK.XSD.Sources.groups()

    groups =
      case args do
        [] ->
          known

        names ->
          by_string = Map.new(known, &{Atom.to_string(&1), &1})

          Enum.map(names, fn n ->
            by_string[n] || Mix.raise("unknown group #{inspect(n)}; known: #{inspect(known)}")
          end)
      end

    Mix.shell().info("Fetching XSDs pinned to SDK tag #{Mix.UOF.SDK.XSD.Sources.sdk_tag()}")

    for group <- groups do
      dir = Mix.UOF.SDK.XSD.Sources.fetch!(group)
      count = dir |> Path.join("**/*.xsd") |> Path.wildcard() |> length()
      Mix.shell().info("#{group}: #{count} xsd -> #{dir}")
    end
  end
end

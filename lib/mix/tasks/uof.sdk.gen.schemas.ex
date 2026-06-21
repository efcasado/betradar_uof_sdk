defmodule Mix.Tasks.Uof.Sdk.Gen.Schemas do
  @moduledoc """
  Generate Ecto embedded schemas for the AMQP feed messages from the XSD
  cached under `priv/xsd/`.

      mix uof.sdk.xsd.fetch     # download the XSD first (pinned SDK tag)
      mix uof.sdk.gen.schemas   # then generate

  Reads the (git-ignored) `priv/xsd/` cache populated by `mix uof.sdk.xsd.fetch`
  and raises if the XSD is missing. One module per `complexType` is generated
  into `lib/uof/sdk/feed/`.
  """
  use Mix.Task

  @shortdoc "Generate feed Ecto embedded schemas from XSDs"

  # {fetch_group, namespace, output dir, roots}
  #
  # `roots` is the allow-list of root *element* names to generate from: only
  # complexTypes reachable from these are emitted. These are the nine AMQP feed
  # message types.
  @groups [
    {:feed, "UOF.SDK.Feed", "lib/uof/sdk/feed",
     ~w(odds_change bet_settlement bet_stop bet_cancel rollback_bet_cancel
        rollback_bet_settlement fixture_change alive snapshot_complete)}
  ]

  @impl Mix.Task
  def run(_args) do
    for {group, namespace, out_dir, roots} <- @groups do
      generate_group(group, namespace, out_dir, roots)
    end
  end

  defp generate_group(group, namespace, out_dir, roots) do
    dir = Mix.UOF.SDK.XSD.Sources.dir(group)

    # Fetch on demand so a fresh checkout can generate without a separate step.
    if Path.wildcard(Path.join(dir, "**/*.xsd")) == [] do
      Mix.UOF.SDK.XSD.Sources.fetch!(group)
    end

    paths = Path.wildcard(Path.join(dir, "**/*.xsd"))

    if paths == [] do
      Mix.raise("no XSDs found in #{dir} for #{group}; run `mix uof.sdk.xsd.fetch` first")
    end

    {types, parsed_roots} = Mix.UOF.SDK.XSD.parse_files(paths)
    types = scope(types, parsed_roots, roots)

    # Regenerate from scratch so pruned/renamed types don't leave stale modules.
    File.rm_rf!(out_dir)
    File.mkdir_p!(out_dir)

    for {short_name, source} <- Mix.UOF.SDK.XSD.Generator.generate(types, namespace) do
      file = Path.join(out_dir, Macro.underscore(short_name) <> ".ex")
      formatted = IO.iodata_to_binary(Code.format_string!(source))
      File.write!(file, formatted <> "\n")
      Mix.shell().info("generated #{file}")
    end
  end

  defp scope(types, _parsed_roots, :all), do: types

  defp scope(types, parsed_roots, roots) do
    root_type_names = Mix.UOF.SDK.XSD.root_types(parsed_roots, roots)
    Mix.UOF.SDK.XSD.reachable_types(types, root_type_names)
  end
end

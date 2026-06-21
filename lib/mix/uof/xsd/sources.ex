defmodule Mix.UOF.XSD.Sources do
  @moduledoc false
  # Upstream XSD sources, pinned to a single SDK release tag so code
  # generation is reproducible. Schemas are downloaded on demand by
  # `mix uof.gen.schemas` and are never vendored into the repo (see
  # `.gitignore`).
  #
  # NOTE: this is intentionally a near-verbatim duplicate of the codegen in
  # `uof_api`, scoped to the AMQP *feed* schema (`UnifiedFeed.xsd`) only.
  # Both copies are expected to be extracted into a shared package later.

  @sdk_repo "sportradar/UnifiedOddsSdkNetCore"
  @sdk_tag "v3.11.0"

  @priv "priv/xsd"

  @groups [:feed]

  def sdk_tag, do: @sdk_tag
  def groups, do: @groups

  @doc "Local directory where a group's XSDs are cached after download."
  def dir(group), do: Path.join(@priv, Atom.to_string(group))

  @doc """
  Download a group's XSDs from the pinned SDK tag into `dir(group)`,
  replacing any previous contents. Returns the directory.
  """
  def fetch!(group) when group in @groups do
    {:ok, _} = Application.ensure_all_started(:req)
    dir = dir(group)
    File.rm_rf!(dir)

    for {sdk_path, dest_rel} <- select(group) do
      dest = Path.join(dir, dest_rel)
      File.mkdir_p!(Path.dirname(dest))
      File.write!(dest, raw!(sdk_path))
    end

    dir
  end

  ## per-group source selection ---------------------------------------------

  # The AMQP feed message schema. Self-contained (no xs:include/xs:import), so a
  # single file is all the codegen needs.
  defp select(:feed) do
    [{"ext/unifiedsdk/xsd/UnifiedFeed.xsd", "UnifiedFeed.xsd"}]
  end

  ## github (pinned tag) -----------------------------------------------------

  defp raw!(path) do
    Req.get!("https://raw.githubusercontent.com/#{@sdk_repo}/#{@sdk_tag}/#{path}", headers: ua()).body
  end

  defp ua, do: [{"user-agent", "uof_sdk-xsd-codegen"}]
end

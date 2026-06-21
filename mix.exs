defmodule BetradarUofSdk.MixProject do
  use Mix.Project

  def project do
    [
      app: :betradar_uof_sdk,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto, "~> 3.12"},
      {:saxy, "~> 1.5"},
      # Used by the XSD codegen mix tasks to download the pinned upstream XSD.
      {:req, "~> 0.5", only: :dev, runtime: false}
    ]
  end
end

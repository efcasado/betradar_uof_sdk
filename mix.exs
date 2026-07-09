defmodule UofSdk.MixProject do
  use Mix.Project

  def project do
    [
      app: :uof_sdk,
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
      {:uof_api, "~> 2.1"},
      {:uof_schemas, "~> 0.2.0"},
      {:broadway, "~> 1.3"},
      {:broadway_rabbitmq, "~> 0.8", optional: true},
      {:off_broadway_pulsar, "~> 1.4", optional: true},
      # dev / test
      {:styler, "~> 1.2", only: [:dev, :test], runtime: false}
    ]
  end
end

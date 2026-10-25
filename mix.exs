defmodule UofSdk.MixProject do
  use Mix.Project

  def project do
    [
      app: :uof_sdk,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
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
      {:uof_api, "~> 3.0"},
      {:uof_schemas, "~> 0.3.0"},
      {:broadway, "~> 1.3"},
      {:nimble_options, "~> 1.1"},
      {:off_broadway_pulsar, "~> 2.0"},
      # dev / test
      {:styler, "~> 1.2", only: [:dev, :test], runtime: false},
      # Integration tests publish to RabbitMQ, upstream of the source connector.
      {:amqp, "~> 4.1", only: :test}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]
end

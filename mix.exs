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
      {:uof_api, "~> 2.0"},
      {:broadway, "~> 1.3"},
      {:broadway_rabbitmq, "~> 0.8"},
      {:ecto, "~> 3.12"},
      {:saxy, "~> 1.5"}
      # `req` (used by the XSD codegen mix tasks) is pulled in transitively by
      # uof_api; `amqp` is pulled in transitively by broadway_rabbitmq.
    ]
  end
end

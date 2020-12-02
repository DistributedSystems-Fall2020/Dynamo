defmodule Dynamo.MixProject do
  use Mix.Project

  def project do
    [
      app: :dynamo,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.10",
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
      {:emulation, in_umbrella: true},
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
      {:credo, "~> 1.4", only: [:dev, :test], runtime: false},
      {:statistics, "~> 0.6.2"},
      {:ex_hash_ring, "~> 3.0"},
      {:bisect, "~> 0.2.0"},
      {:phst_transform, "~> 1.0"}
    ]
  end
end

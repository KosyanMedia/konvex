defmodule Konvex.MixProject do
  use Mix.Project

  def project do
    [
      app: :konvex,
      version: "1.1.1",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:riakc, github: "basho/riak-erlang-client", tag: "3.0.8+p1"}
    ]
  end
end

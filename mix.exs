defmodule Konvex.MixProject do
  use Mix.Project

  def project do
    [
      app: :konvex,
      version: "1.1.7",
      elixir: "~> 1.0",
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
      # FIXME: This is a workaround to force skip :protobuffs transitive dependency
      {:riak_pb, github: "basho/riak_pb", ref: "7a5e535217c13a32f3041888b0d46e9b4476065c", override: true},
      {:riakc, github: "basho/riak-erlang-client", tag: "3.0.13"}
    ]
  end
end

defmodule Konvex.MixProject do
  use Mix.Project

  def project do
    [
      app: :konvex,
      version: "1.1.6",
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
      # FIXME: This is a workaround to force skip :protobuffs transitive dependency
      {
        :riak_pb,
        github: "basho/riak_pb",
        ref: "46d5f4d1b899fbde4357f9d777a8c71d8ac66a35",
        override: true,
        # NOTE: This is a workaround for building riak_pb with Elixir >= 1.13
        compile:
          "#{Mix.Rebar.local_rebar_path(:rebar3)} bare compile --paths #{Mix.Project.build_path()}/lib/*/ebin"
      },
      {:riakc, github: "basho/riak-erlang-client", tag: "3.0.8+p1"}
    ]
  end
end

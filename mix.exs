defmodule Konvex.MixProject do
  use Mix.Project

  def project do
    [
      app: :konvex,
      version: "1.1.0",
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
      # :riak is outdated wrapper for :riak-erlang-client
      # (furthermore it utilizes incompatible with OTP 24 :pooler)
      # TODO: Use https://github.com/basho/riak-erlang-client directly instead
      {:riak, "~> 1.1.6"}
    ]
  end
end

defmodule Konvex.Implementation.Riak.ConflictResolutionStrategy.LastWriteWinsTest do
  use ExUnit.Case
  doctest Konvex.Implementation.Riak.ConflictResolutionStrategy.LastWriteWins

  defmodule BackendMock do
    use GenServer

    def start_link([]) do
      GenServer.start_link(__MODULE__, [], [])
    end

    @impl GenServer
    def init([]) do
      {:ok, :respond_with_conflicts}
    end

    @impl GenServer
    def handle_call(
          {
            :req,
            {
              :rpbgetreq,
              "probe_bucket",
              <<_, _ :: binary>> = bucket_key,
              :undefined,
              :undefined,
              :undefined,
              :undefined,
              :undefined,
              :undefined,
              :undefined,
              :undefined,
              :undefined,
              :undefined,
              :undefined
            },
            _timeout
          },
          _from,
          :respond_with_conflicts
        ) do
      casual_context =
        <<42>>
      metadata =
        :dict.new()
      {
        :reply,
        {
          :ok,
          {
            :riakc_obj,
            "probe_bucket",
            bucket_key,
            casual_context,
            [{metadata, "the first one"}, {metadata, "the last one"}],
            # Uncommitted metadata update
            :undefined,
            # Uncommitted value update
            :undefined
          }
        },
        :respond_with_conflicts
      }
    end
  end

  defmodule Client do
    use Konvex.Implementation.Riak.Ability.ToGetTextValue,
        bucket_name: "probe_bucket",
        conflict_resolution_strategy_module: Konvex.Implementation.Riak.ConflictResolutionStrategy.LastWriteWins,
        connection_provider: Application.get_env(:konvex, :backend_mock_pid)
  end

  setup_all do
    backend_mock_pid =
      start_supervised!({BackendMock, []})
    Application.put_env(:konvex, :backend_mock_pid, backend_mock_pid)
  end

  describe "LastWriteWins" do
    test "should return the last value from the sibling values list" do
      assert Client.get_text_value("foo") === "the last one"
    end
  end
end

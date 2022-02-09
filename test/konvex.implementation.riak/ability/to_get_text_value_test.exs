defmodule Konvex.Implementation.Riak.Ability.ToGetTextValueTest do
  use ExUnit.Case
  doctest Konvex.Implementation.Riak.Ability.ToGetTextValue

  defmodule BackendMock do
    use GenServer

    def start_link([%{} = storage_initial_state]) do
      GenServer.start_link(__MODULE__, storage_initial_state, [])
    end

    @impl GenServer
    def init(%{} = storage_initial_state) do
      {:ok, storage_initial_state}
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
              :undefined,
              :undefined
            },
            _timeout
          },
          _from,
          %{} = storage_state
        ) do
      {
        :reply,
        case storage_state |> Map.get(bucket_key) do
          nil ->
            {:error, :notfound}

          the_only_value ->
            casual_context =
              <<42>>
            metadata =
              :dict.new()
            {
              :ok,
              {
                :riakc_obj,
                "probe_bucket",
                bucket_key,
                casual_context,
                [{metadata, the_only_value}],
                # Uncommitted metadata update
                :undefined,
                # Uncommitted value update
                :undefined
              }
            }
        end,
        storage_state
      }
    end
  end

  defmodule Client do
    use Konvex.Implementation.Riak.Ability.ToGetTextValue,
        bucket_name: "probe_bucket",
        conflict_resolution_strategy_module: Konvex.Implementation.Riak.ConflictResolutionStrategy.Raise,
        connection: Application.get_env(:konvex, :backend_mock_pid)
  end

  setup_all do
    initial_storage_state =
      %{"foo" => "bar", "bar" => nil}
    backend_mock_pid =
      start_supervised!({BackendMock, [initial_storage_state]})
    Application.put_env(:konvex, :backend_mock_pid, backend_mock_pid)
  end

  describe "Riak ability to get text value" do
    test "should return text value associated with the provided key" do
      assert Client.get_text_value("foo") === "bar"
    end

    test "should return :key_not_found if the provided key is associated with nil" do
      assert Client.get_text_value("bar") === :key_not_found
    end

    test "should return :key_not_found if the provided key is no present" do
      assert Client.get_text_value("baz") === :key_not_found
    end
  end
end

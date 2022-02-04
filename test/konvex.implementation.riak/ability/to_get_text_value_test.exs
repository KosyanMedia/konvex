defmodule Konvex.Implementation.Riak.Ability.ToGetTextValueTest do
  use ExUnit.Case
  # TODO: Refactor to Konvex.Implementation.Riak.Ability.ToGetTextValue is coming
  doctest Konvex.Implementation.Riak.Ability.ToGetValue

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

          value ->
            casual_context =
              <<42>>
            {
              :ok,
              {
                :riakc_obj,
                "probe_bucket",
                bucket_key,
                casual_context,
                [],
                :dict.new(),
                value
              }
            }
        end,
        storage_state
      }
    end
  end

  defmodule Client do
    use Konvex.Implementation.Riak.Ability.ToGetValue,
        bucket_name: "probe_bucket",
        connection_provider: Application.get_env(:konvex, :backend_mock_pid),
        value_type: :text
  end

  describe "Riak ability to get text value" do
    setup do
      initial_storage_state =
        %{"foo" => "bar", "bar" => nil}
      backend_mock_pid =
        start_supervised!({BackendMock, [initial_storage_state]})
      Application.put_env(:konvex, :backend_mock_pid, backend_mock_pid)
    end

    test "should return text value associated with the provided key" do
      assert Client.get("foo") === "bar"
    end

    test "should return :key_not_found if the provided key is associated with nil" do
      assert Client.get("bar") === :key_not_found
    end

    test "should return :key_not_found if the provided key is no present" do
      assert Client.get("baz") === :key_not_found
    end

    # TODO: Siblings problem
  end
end

defmodule Konvex.Implementation.Riak.Ability.ToAddSetValue do
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection_provider: quoted_riak_connection_provider,
               set_type_name: <<_, _ :: binary>> = set_type_name
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      @behaviour Konvex.Ability.ToAddSetValue

      @impl Konvex.Ability.ToAddSetValue
      @spec add(key :: String.t, value :: String.t) :: :key_not_found | :unit
      def add(<<_, _ :: binary>> = key, value) when is_binary(value) do
        connection_pid =
          Connection.Provider.get_connection_pid(unquote(quoted_riak_connection_provider))
        Riak.find(
          connection_pid,
          unquote(set_type_name),
          unquote(bucket_name),
          key
        )
        |> case do
             {
               :set,
               fetched_set_values,
               [] = _uncommitted_added_set_values,
               [] = _uncommitted_removed_set_values,
               casual_context_to_preserve
             } = fetched_set when is_list(fetched_set_values) and is_binary(casual_context_to_preserve) ->
               fetched_set
               |> Riak.CRDT.Set.put(value)
               |> (
                    fn new_key_value_object ->
                      Riak.update(
                        connection_pid,
                        new_key_value_object,
                        unquote(set_type_name),
                        unquote(bucket_name),
                        key
                      )
                    end
                    ).()
               |> case do
                    :ok ->
                      :unit

                    # Formally it has three successful term more
                  end

             nil ->
               :key_not_found

             {:error, some_reason_from_riakc_pb_socket_fetch_type} ->
               raise "Failed to find #{unquote(bucket_name)}<#{unquote(set_type_name)}>:#{key} in Riak, :riakc_pb_socket.fetch_type responded: #{inspect some_reason_from_riakc_pb_socket_fetch_type}"
           end
      end
    end
  end
end

defmodule Konvex.Implementation.Riak.Ability.ToRemoveSetValue do
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection_provider: quoted_riak_connection_provider,
               set_type_name: <<_, _ :: binary>> = set_type_name
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      @behaviour Konvex.Ability.ToRemoveSetValue

      @impl Konvex.Ability.ToRemoveSetValue
      @spec remove(key :: String.t, value :: String.t) :: :key_not_found | :unit
      def remove(<<_, _ :: binary>> = key, value) when is_binary(value) do
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
               |> Riak.CRDT.Set.delete(value)
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

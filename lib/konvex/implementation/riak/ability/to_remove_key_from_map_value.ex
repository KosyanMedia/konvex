defmodule Konvex.Implementation.Riak.Ability.ToRemoveKeyFromMapValue do
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection_provider: quoted_riak_connection_provider,
               map_type_name: <<_, _ :: binary>> = map_type_name
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      @behaviour Konvex.Ability.ToRemoveKeyFromMapValue

      @impl Konvex.Ability.ToRemoveKeyFromMapValue
      @spec remove_key_from_map_value(key :: String.t, map_key :: String.t) :: :key_not_found | :unit
      def remove_key_from_map_value(key, map_key) when is_binary(key) and is_binary(map_key) do
        connection_pid =
          Connection.Provider.get_connection_pid(unquote(quoted_riak_connection_provider))
        Riak.find(
          connection_pid,
          unquote(map_type_name),
          unquote(bucket_name),
          key
        )
        |> case do
             {
               :map,
               fetched_map_entries,
               [] = _uncommitted_added_map_entries,
               [] = _uncommitted_removed_map_keys,
               casual_context_to_preserve
             } = fetched_map when is_list(fetched_map_entries) and is_binary(casual_context_to_preserve) ->
               case fetched_map_entries
                    |> Enum.find(
                         fn {{entry_key, entry_type}, entry_value}
                            when is_binary(entry_key) and is_atom(entry_type) and is_binary(entry_value) ->
                           entry_key === map_key
                         end
                       ) do
                 nil ->
                   # Idempotent operation
                   :ok

                 {{matched_entry_key, matched_entry_type}, _matched_entry_value} ->
                   Riak.update(
                     connection_pid,
                     {
                       :map,
                       fetched_map_entries,
                       [],
                       [{matched_entry_key, matched_entry_type}],
                       casual_context_to_preserve
                     },
                     unquote(map_type_name),
                     unquote(bucket_name),
                     key
                   )
               end
               |> case do
                    :ok ->
                      :unit

                    # Formally Riak.update/5 has three successful term more
                  end

             nil ->
               :key_not_found

             {:error, some_reason_from_riakc_pb_socket_fetch_type} ->
               raise "Failed to find #{unquote(bucket_name)}<#{unquote(map_type_name)}>:#{key} in Riak, :riakc_pb_socket.fetch_type responded: #{inspect some_reason_from_riakc_pb_socket_fetch_type}"
           end
      end
    end
  end
end

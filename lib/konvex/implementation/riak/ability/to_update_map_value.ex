defmodule Konvex.Implementation.Riak.Ability.ToUpdateMapValue do
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection_provider: quoted_riak_connection_provider,
               map_type_name: <<_, _ :: binary>> = map_type_name
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      @behaviour Konvex.Ability.ToUpdateMapValue

      @impl Konvex.Ability.ToUpdateMapValue
      @spec update(key :: String.t, map_key :: String.t, update :: (old_value :: String.t -> new_value :: String.t))
            :: :key_not_found | :unit
      def update(<<_, _ :: binary>> = key, <<_, _ :: binary>> = map_key, update) do
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
               fetched_map
               |> Riak.CRDT.Map.update(
                    :register,
                    map_key,
                    fn {:register, old_value, :undefined} = entry_value when
                         is_binary(old_value) ->
                      entry_value
                      |> Riak.CRDT.Register.set(update.(old_value))
                    end
                  )
               |> (
                    fn map_with_updated_key ->
                      Riak.update(
                        connection_pid,
                        map_with_updated_key,
                        unquote(map_type_name),
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
               raise "Failed to find #{unquote(bucket_name)}<#{unquote(map_type_name)}>:#{key} in Riak, :riakc_pb_socket.fetch_type responded: #{inspect some_reason_from_riakc_pb_socket_fetch_type}"
           end
      end
    end
  end
end

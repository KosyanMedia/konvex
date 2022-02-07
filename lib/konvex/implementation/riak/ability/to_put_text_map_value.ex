defmodule Konvex.Implementation.Riak.Ability.ToPutTextMapValue do
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection_provider: quoted_riak_connection_provider,
               map_type_name: <<_, _ :: binary>> = map_type_name
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      @behaviour Konvex.Ability.ToPutTextMapValue

      @impl Konvex.Ability.ToPutTextMapValue
      @spec put_text_map_value(key :: String.t, value :: %{key :: String.t => value :: String.t}) :: :unit
      def put_text_map_value(key, empty_map) when is_binary(key) and empty_map === %{} do
        connection_pid =
          Connection.Provider.get_connection_pid(unquote(quoted_riak_connection_provider))
        case Riak.find(
               connection_pid,
               unquote(map_type_name),
               unquote(bucket_name),
               key
             ) do
          {
            :map,
            [] = _fetched_map_entries,
            [] = _uncommitted_added_map_entries,
            [] = _uncommitted_removed_map_keys,
            casual_context
          } when is_binary(casual_context) ->
            # Idempotent operation
            :ok

          {
            :map,
            fetched_map_entries,
            [] = _uncommitted_added_map_entries,
            [] = _uncommitted_removed_map_keys,
            casual_context_to_preserve
          } = fetched_map when is_list(fetched_map_entries) and is_binary(casual_context_to_preserve) ->
            # In this case we have to remove every already present map entry
            Riak.update(
              connection_pid,
              {
                :map,
                fetched_map_entries,
                [],
                fetched_map_entries
                |> Enum.map(
                     fn {{entry_key, entry_type} = fetched_map_entry, entry_value}
                        when is_binary(entry_key) and is_atom(entry_type) and is_binary(entry_value) ->
                       fetched_map_entry
                     end
                   ),
                casual_context_to_preserve
              },
              unquote(map_type_name),
              unquote(bucket_name),
              key
            )

          nil ->
            # In this case we have to commit an empty map
            # This can't be accomplished using :riakc itself
            # (library forbids "unmodified commits", see to_op/1, update_type/5, etc.)
            # So we workaround this by creating a map with probe entry and then remove it from the map
            with new_map_with_probe_entry <-
                   Riak.CRDT.Map.new()
                   |> Riak.CRDT.Map.put(
                        "probe_key",
                        Riak.CRDT.Register.new("probe_value")
                      ),
                 :ok <-
                   Riak.update(
                     connection_pid,
                     new_map_with_probe_entry,
                     unquote(map_type_name),
                     unquote(bucket_name),
                     key
                   ),
                 {
                   :map,
                   [{{"probe_key", :register} = probe_entry_key, "probe_value"}],
                   [] = _uncommitted_added_map_entries,
                   [] = _uncommitted_removed_map_keys,
                   casual_context_to_preserve
                 } = persisted_new_map_with_probe_entry when is_binary(casual_context_to_preserve) <-
                   Riak.find(
                     connection_pid,
                     unquote(map_type_name),
                     unquote(bucket_name),
                     key
                   ),
                 empty_map <-
                   Riak.CRDT.Map.delete(
                     persisted_new_map_with_probe_entry,
                     probe_entry_key
                   ) do
              Riak.update(
                connection_pid,
                empty_map,
                unquote(map_type_name),
                unquote(bucket_name),
                key
              )
            end

          {:error, some_reason_from_riakc_pb_socket_fetch_type} ->
            raise "Failed to find #{unquote(bucket_name)}<#{unquote(map_type_name)}>:#{key} in Riak, :riakc_pb_socket.fetch_type responded: #{inspect some_reason_from_riakc_pb_socket_fetch_type}"
        end
        |> case do
             :ok ->
               :unit

             # Formally Riak.update/5 has three successful term more
           end
      end

      def put_text_map_value(key, %{} = nonempty_map) when is_binary(key) do
        connection_pid =
          Connection.Provider.get_connection_pid(unquote(quoted_riak_connection_provider))
        case Riak.find(
               connection_pid,
               unquote(map_type_name),
               unquote(bucket_name),
               key
             ) do
          {
            :map,
            fetched_map_entries,
            [] = _uncommitted_added_map_entries,
            [] = _uncommitted_removed_map_keys,
            casual_context_to_preserve
          } = fetched_map when is_list(fetched_map_entries) and is_binary(casual_context_to_preserve) ->
            {
              :map,
              fetched_map_entries,
              [],
              fetched_map_entries
              |> Enum.filter(
                   fn {{entry_key, entry_type}, entry_value}
                      when is_binary(entry_key) and is_atom(entry_type) and is_binary(entry_value) ->
                     not Map.has_key?(nonempty_map, entry_key)
                   end
                 )
              |> Enum.map(fn {entry_key_and_type, entry_value} -> entry_key_and_type end),
              casual_context_to_preserve
            }

          nil ->
            Riak.CRDT.Map.new()

          {:error, some_reason_from_riakc_pb_socket_fetch_type} ->
            raise "Failed to find #{unquote(bucket_name)}<#{unquote(map_type_name)}>:#{key} in Riak, :riakc_pb_socket.fetch_type responded: #{inspect some_reason_from_riakc_pb_socket_fetch_type}"
        end
        |> (
             fn map_without_redundant_keys ->
               nonempty_map
               |> Enum.reduce(
                    map_without_redundant_keys,
                    fn {entry_key, entry_value}, map_extended_with_new_map_entries
                    when is_binary(entry_key) and is_binary(entry_value) ->
                      map_extended_with_new_map_entries
                      |> Riak.CRDT.Map.update(
                           :register,
                           entry_key,
                           fn {:register, old_value, :undefined} when is_binary(old_value) ->
                             {:register, old_value, entry_value}
                           end
                         )
                    end
                  )
             end
             ).()
        |> (
             fn new_map ->
               Riak.update(
                 connection_pid,
                 new_map,
                 unquote(map_type_name),
                 unquote(bucket_name),
                 key
               )
             end
             ).()
        |> case do
             :ok ->
               :unit

             # Formally Riak.update/5 has three successful term more
           end
      end
    end
  end
end

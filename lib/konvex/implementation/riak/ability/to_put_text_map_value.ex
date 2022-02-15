defmodule Konvex.Implementation.Riak.Ability.ToPutTextMapValue do
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection: quoted_riak_connection,
               map_type_name: <<_, _ :: binary>> = map_type_name
             ]
           ) do
    quote do
      import Konvex.Implementation.Riak.Connection.Usage, only: [using: 2]

      @behaviour Konvex.Ability.ToPutTextMapValue

      @impl Konvex.Ability.ToPutTextMapValue
      @spec put_text_map_value(key :: String.t, value :: %{key :: String.t => value :: String.t}) :: :unit
      def put_text_map_value("" = _empty_key, %{} = _map) do
        raise ":riakc does not support empty keys"
      end

      def put_text_map_value(<<_, _ :: binary>> = key, empty_map) when empty_map === %{} do
        using unquote(quoted_riak_connection), fn connection_pid ->
          case :riakc_pb_socket.fetch_type(
                 connection_pid,
                 {unquote(map_type_name), unquote(bucket_name)},
                 key
               ) do
            {
              :ok,
              {
                :map,
                [] = _fetched_map_entries,
                [] = _uncommitted_added_map_entries,
                [] = _uncommitted_removed_map_keys,
                casual_context_that_has_to_be_preserved
              }
            } when is_binary(casual_context_that_has_to_be_preserved) ->
              # Idempotent operation
              :ok

            {
              :ok,
              {
                :map,
                fetched_map_entries,
                [] = _uncommitted_added_map_entries,
                [] = _uncommitted_removed_map_keys,
                casual_context_that_has_to_be_preserved
              }
            } when is_list(fetched_map_entries) and is_binary(casual_context_that_has_to_be_preserved) ->
              # In this case we have to remove every already present map entry
              :riakc_pb_socket.update_type(
                connection_pid,
                {unquote(map_type_name), unquote(bucket_name)},
                key,
                :riakc_map.to_op(
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
                    casual_context_that_has_to_be_preserved
                  }
                )
              )

            {:error, {:notfound, :map}} ->
              # In this case we have to commit an empty map
              # This can't be accomplished using :riakc itself
              # (library forbids "unmodified commits", see to_op/1, update_type/5, etc.)
              # So we workaround this by creating a map with probe entry and then remove it from the map
              with new_map_with_probe_entry <-
                     {
                       :map,
                       [],
                       [{{"probe_key", :register}, {:register, :undefined, "probe_value"}}],
                       [],
                       :undefined
                     },
                   :ok <-
                     :riakc_pb_socket.update_type(
                       connection_pid,
                       {unquote(map_type_name), unquote(bucket_name)},
                       key,
                       :riakc_map.to_op(new_map_with_probe_entry)
                     ),
                   {
                     :ok,
                     {
                       :map,
                       [{{"probe_key", :register} = probe_entry_key, "probe_value"}],
                       [] = _uncommitted_added_map_entries,
                       [] = _uncommitted_removed_map_keys,
                       casual_context_that_has_to_be_preserved
                     }
                   } = persisted_new_map_with_probe_entry when is_binary(casual_context_that_has_to_be_preserved) <-
                     :riakc_pb_socket.fetch_type(
                       connection_pid,
                       {unquote(map_type_name), unquote(bucket_name)},
                       key
                     ),
                   empty_map <-
                     :riakc_map.erase(probe_entry_key, persisted_new_map_with_probe_entry) do
                :riakc_pb_socket.update_type(
                  connection_pid,
                  {unquote(map_type_name), unquote(bucket_name)},
                  key,
                  :riakc_map.to_op(empty_map)
                )
              end

            {:error, riakc_pb_socket_fetch_type_error} ->
              object_locator =
                "#{unquote(bucket_name)}<#{unquote(map_type_name)}>:#{key}"
              error_message =
                inspect riakc_pb_socket_fetch_type_error
              raise "Failed to find #{object_locator} in Riak, :riakc_pb_socket.fetch_type/3 responded: #{error_message}"
          end
          |> case do
               :ok ->
                 :unit
             end
        end
      end

      def put_text_map_value(<<_, _ :: binary>> = key, %{} = nonempty_map) do
        using unquote(quoted_riak_connection), fn connection_pid ->
          case :riakc_pb_socket.fetch_type(
                 connection_pid,
                 {unquote(map_type_name), unquote(bucket_name)},
                 key
               ) do
            {
              :ok,
              {
                :map,
                [
                  {{<<_, _ :: binary>> = _entry_key, _entry_type}, <<_ :: binary>> = _entry_value}
                  | _rest_entries
                ] = fetched_map_entries,
                [] = _uncommitted_added_map_entries,
                [] = _uncommitted_removed_map_keys,
                casual_context_that_has_to_be_preserved
              }
            } when is_binary(casual_context_that_has_to_be_preserved) ->
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
                casual_context_that_has_to_be_preserved
              }

            {:error, {:notfound, :map}} ->
              {:map, [], [], [], :undefined}

            {:error, riakc_pb_socket_fetch_type_error} ->
              object_locator =
                "#{unquote(bucket_name)}<#{unquote(map_type_name)}>:#{key}"
              error_message =
                inspect riakc_pb_socket_fetch_type_error
              raise "Failed to find #{object_locator} in Riak, :riakc_pb_socket.fetch_type/3 responded: #{error_message}"
          end
          |> (
               fn map_without_redundant_keys ->
                 nonempty_map
                 |> Enum.reduce(
                      map_without_redundant_keys,
                      fn {entry_key, entry_value}, map_extended_with_new_map_entries
                      when is_binary(entry_key) and is_binary(entry_value) ->
                        :riakc_map.update(
                          {entry_key, :register},
                          fn {:register, old_value, :undefined} when is_binary(old_value) ->
                            {:register, old_value, entry_value}
                          end,
                          map_extended_with_new_map_entries
                        )
                      end
                    )
               end
               ).()
          |> (
               fn new_map ->
                 :riakc_pb_socket.update_type(
                   connection_pid,
                   {unquote(map_type_name), unquote(bucket_name)},
                   key,
                   :riakc_map.to_op(new_map)
                 )
               end
               ).()
          |> case do
               :ok ->
                 :unit
             end
        end
      end
    end
  end
end

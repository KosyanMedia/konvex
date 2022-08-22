defmodule Konvex.Implementation.Riak.Ability.ToPutValueToTextMapValue do
  defmacro __using__(
             [
               bucket_name: quoted_bucket_name,
               connection: quoted_riak_connection,
               map_type_name: quoted_map_type_name
             ]
           ) do
    quote do
      import Konvex.Implementation.Riak.Connection.Usage, only: [using: 2]

      @behaviour Konvex.Ability.ToPutValueToTextMapValue

      @impl Konvex.Ability.ToPutValueToTextMapValue
      @spec put_value_to_text_map_value(key :: String.t, map_key :: String.t, value :: String.t)
            :: :key_not_found | :unit
      def put_value_to_text_map_value(key, map_key, <<_ :: binary>> = value)
          when key === "" or map_key === "" do
        raise ":riakc does not support empty keys"
      end

      def put_value_to_text_map_value(<<_, _ :: binary>> = key, <<_, _ :: binary>> = map_key, value)
          when is_binary(value) do
        using unquote(quoted_riak_connection), fn connection_pid ->
          case :riakc_pb_socket.fetch_type(
                 connection_pid,
                 {unquote(quoted_map_type_name), unquote(quoted_bucket_name)},
                 key
               ) do
            {
              :ok,
              {
                :map,
                fetched_map_entries,
                [] = _uncommitted_added_map_entries,
                [] = _uncommitted_removed_map_keys,
                casual_context_that_has_to_be_preserved
              } = fetched_map
            } when is_list(fetched_map_entries) and is_binary(casual_context_that_has_to_be_preserved) ->
              :riakc_pb_socket.update_type(
                connection_pid,
                {unquote(quoted_map_type_name), unquote(quoted_bucket_name)},
                key,
                :riakc_map.to_op(
                  :riakc_map.update(
                    {map_key, :register},
                    fn {:register, old_value, :undefined} when is_binary(old_value) ->
                      {:register, old_value, value}
                    end,
                    fetched_map
                  )
                )
              )
              |> case do
                   :ok ->
                     :unit

                   {:error, riakc_pb_socket_update_type_error} ->
                     object_locator =
                       "#{unquote(quoted_bucket_name)}<#{unquote(quoted_map_type_name)}>:#{key}"
                     error_message =
                       inspect riakc_pb_socket_update_type_error
                     raise "Failed to update #{object_locator} in Riak, :riakc_pb_socket.update_type/4 responded: #{error_message}"
                 end

            {:error, {:notfound, :map}} ->
              :key_not_found

            {:error, riakc_pb_socket_fetch_type_error} ->
              object_locator =
                "#{unquote(quoted_bucket_name)}<#{unquote(quoted_map_type_name)}>:#{key}"
              error_message =
                inspect riakc_pb_socket_fetch_type_error
              raise "Failed to find #{object_locator} in Riak, :riakc_pb_socket.fetch_type/3 responded: #{error_message}"
          end
        end
      end
    end
  end
end

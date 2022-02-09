defmodule Konvex.Implementation.Riak.Ability.ToGetTextMapValue do
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection: quoted_riak_connection,
               map_type_name: <<_, _ :: binary>> = map_type_name
             ]
           ) do
    quote do
      import Konvex.Implementation.Riak.Connection.Usage, only: [using: 2]

      @behaviour Konvex.Ability.ToGetTextMapValue

      @impl Konvex.Ability.ToGetTextMapValue
      @spec get_text_map_value(key :: String.t) :: :key_not_found | %{key :: String.t => value :: String.t}
      def get_text_map_value("" = _empty_key) do
        # :riakc does not support empty keys, so no way to store such
        :key_not_found
      end

      def get_text_map_value(<<_, _ :: binary>> = key) do
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
                fetched_map_entries,
                [] = _uncommitted_added_map_entries,
                [] = _uncommitted_removed_map_keys,
                casual_context
              }
            } when is_list(fetched_map_entries) and is_binary(casual_context) ->
              fetched_map_entries
              |> Enum.map(
                   fn {{entry_key, :register}, entry_value} when is_binary(entry_key) and is_binary(entry_value) ->
                     {entry_key, entry_value}
                   end
                 )
              |> Map.new()

            {:error, {:notfound, :map}} ->
              :key_not_found

            {:error, riakc_pb_socket_fetch_type_error} ->
              object_locator =
                "#{unquote(bucket_name)}<#{unquote(map_type_name)}>:#{key}"
              error_message =
                inspect riakc_pb_socket_fetch_type_error
              raise "Failed to find #{object_locator} in Riak, :riakc_pb_socket.fetch_type/3 responded: #{error_message}"
          end
        end
      end
    end
  end
end

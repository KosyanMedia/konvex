defmodule Konvex.Implementation.Riak.Ability.ToGetTextMapValue do
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection_provider: quoted_riak_connection_provider,
               map_type_name: <<_, _ :: binary>> = map_type_name
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      @behaviour Konvex.Ability.ToGetTextMapValue

      @impl Konvex.Ability.ToGetTextMapValue
      @spec get_text_map_value(key :: String.t) :: :key_not_found | %{key :: String.t => value :: String.t}
      def get_text_map_value(key) when is_binary(key) do
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
            casual_context
          } when is_list(fetched_map_entries) and is_binary(casual_context) ->
            fetched_map_entries
            |> Enum.map(
                 fn {{entry_key, :register}, entry_value} when is_binary(entry_key) and is_binary(entry_value) ->
                   {entry_key, entry_value}
                 end
               )
            |> Map.new()

          nil ->
            :key_not_found

          {:error, some_reason_from_riakc_pb_socket_fetch_type} ->
            raise "Failed to find #{unquote(bucket_name)}<#{unquote(map_type_name)}>:#{key} in Riak, :riakc_pb_socket.fetch_type responded: #{inspect some_reason_from_riakc_pb_socket_fetch_type}"
        end
      end
    end
  end
end

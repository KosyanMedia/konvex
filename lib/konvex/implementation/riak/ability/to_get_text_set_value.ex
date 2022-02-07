defmodule Konvex.Implementation.Riak.Ability.ToGetTextSetValue do
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection: quoted_riak_connection,
               set_type_name: <<_, _ :: binary>> = set_type_name
             ]
           ) do
    quote do
      import Konvex.Implementation.Riak.Connection.Usage, only: [using: 2]

      @behaviour Konvex.Ability.ToGetTextSetValue

      @impl Konvex.Ability.ToGetTextSetValue
      @spec get_text_set_value(key :: String.t) :: :key_not_found | MapSet.t(String.t)
      def get_text_set_value("" = _empty_key) do
        # :riakc does not support empty keys, so no way to store such
        :key_not_found
      end

      def get_text_set_value(<<_, _ :: binary>> = key) do
        using unquote(quoted_riak_connection), fn connection_pid ->
          case Riak.find(
                 connection_pid,
                 unquote(set_type_name),
                 unquote(bucket_name),
                 key
               ) do
            {
              :set,
              fetched_set_values,
              [] = _uncommitted_added_set_values,
              [] = _uncommitted_removed_set_values,
              casual_context
            } = fetched_set when is_list(fetched_set_values) and is_binary(casual_context) ->
              fetched_set_values
              |> MapSet.new()

            nil ->
              :key_not_found

            {:error, some_reason_from_riakc_pb_socket_fetch_type} ->
              raise "Failed to find #{unquote(bucket_name)}<#{unquote(set_type_name)}>:#{key} in Riak, :riakc_pb_socket.fetch_type responded: #{inspect some_reason_from_riakc_pb_socket_fetch_type}"
          end
        end
      end
    end
  end
end

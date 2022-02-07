defmodule Konvex.Implementation.Riak.Ability.ToGetTextValue do
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               conflict_resolution_strategy_module: conflict_resolution_strategy_module,
               connection: quoted_riak_connection
             ]
           ) do
    quote do
      import Konvex.Implementation.Riak.Connection.Usage, only: [using: 2]

      @behaviour Konvex.Ability.ToGetTextValue

      @impl Konvex.Ability.ToGetTextValue
      @spec get_text_value(key :: String.t) :: :key_not_found | String.t
      def get_text_value("" = _empty_key) do
        # :riakc does not support empty keys, so no way to store such
        :key_not_found
      end

      def get_text_value(<<_, _ :: binary>> = key) do
        using unquote(quoted_riak_connection), fn connection_pid ->
          case Riak.find(
                 connection_pid,
                 unquote(bucket_name),
                 key
               ) do
            %Riak.Object{
              data: value
            } when is_binary(value) ->
              value

            nil ->
              :key_not_found

            {:error, some_reason_from_riakc_pb_socket_get} ->
              raise "Failed to find #{unquote(bucket_name)}:#{key} in Riak, :riakc_pb_socket.get responded: #{inspect some_reason_from_riakc_pb_socket_get}"

            [_ | _] = conflicting_sibling_values ->
              # https://docs.riak.com/riak/kv/2.2.3/developing/usage/conflict-resolution/#siblings-in-action
              apply(
                unquote(conflict_resolution_strategy_module),
                :resolve,
                [conflicting_sibling_values, unquote(bucket_name), key]
              )
          end
        end
      end
    end
  end
end

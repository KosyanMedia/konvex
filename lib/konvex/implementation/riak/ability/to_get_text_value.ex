defmodule Konvex.Implementation.Riak.Ability.ToGetTextValue do
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               conflict_resolution_strategy_module: conflict_resolution_strategy_module,
               connection_provider: quoted_riak_connection_provider
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      @behaviour Konvex.Ability.ToGetTextValue

      @impl Konvex.Ability.ToGetTextValue
      @spec get_text_value(key :: String.t) :: :key_not_found | String.t
      def get_text_value(key) when is_binary(key) do
        connection_pid =
          Connection.Provider.get_connection_pid(unquote(quoted_riak_connection_provider))
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

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
          case :riakc_pb_socket.get(connection_pid, unquote(bucket_name), key) do
            {:ok, riakc_obj} ->
              case riakc_obj do
                {
                  :riakc_obj,
                  unquote(bucket_name),
                  object_key,
                  causal_context,
                  [{value_metadata_as_erlang_dict, value}],
                  # Uncommitted new value metadata as Erlang dict
                  :undefined,
                  # Uncommitted new value
                  :undefined
                } when object_key === key and is_binary(causal_context) and is_binary(value) ->
                  value

                {
                  :riakc_obj,
                  unquote(bucket_name),
                  object_key,
                  causal_context,
                  [
                    {_first_sibling_value_metadata_as_erlang_dict, first_sibling_value}
                    | _rest_sibling_values
                  ] = conflicting_sibling_values,
                  # Uncommitted new value metadata as Erlang dict
                  :undefined,
                  # Uncommitted new value
                  :undefined
                } when object_key === key and is_binary(causal_context) and is_binary(first_sibling_value) ->
                  # https://docs.riak.com/riak/kv/2.2.3/developing/usage/conflict-resolution/#siblings-in-action
                  apply(
                    unquote(conflict_resolution_strategy_module),
                    :resolve,
                    [
                      conflicting_sibling_values
                      |> Enum.reduce(
                           [],
                           fn {_metadata, value}, from_most_recent_to_oldest when is_binary(value) ->
                             [value | from_most_recent_to_oldest]
                           end
                         ),
                      unquote(bucket_name),
                      key
                    ]
                  )
              end

            {:error, :notfound} ->
              :key_not_found

            {:error, riakc_pb_socket_get_error} ->
              object_locator =
                "#{unquote(bucket_name)}:#{key}"
              error_message =
                inspect riakc_pb_socket_get_error
              raise "Failed to find #{object_locator} in Riak, :riakc_pb_socket.get/3 responded: #{error_message}"
          end
        end
      end
    end
  end
end

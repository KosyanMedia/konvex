defmodule Konvex.Implementation.Riak.Ability.ToRemoveValueFromTextSetValue do
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection: quoted_riak_connection,
               set_type_name: <<_, _ :: binary>> = set_type_name
             ]
           ) do
    quote do
      import Konvex.Implementation.Riak.Connection.Usage, only: [using: 2]

      @behaviour Konvex.Ability.ToRemoveValueFromTextSetValue

      @impl Konvex.Ability.ToRemoveValueFromTextSetValue
      @spec remove_value_from_text_set_value(key :: String.t, value :: String.t) :: :key_not_found | :unit
      def remove_value_from_text_set_value("" = _empty_key, value) when is_binary(value) do
        # :riakc does not support empty keys, so no way to store such
        :key_not_found
      end

      def remove_value_from_text_set_value(<<_, _ :: binary>> = key, value) when is_binary(value) do
        using unquote(quoted_riak_connection), fn connection_pid ->
          case :riakc_pb_socket.fetch_type(
                 connection_pid,
                 {unquote(set_type_name), unquote(bucket_name)},
                 key
               ) do
            {
              :ok,
              {
                :set,
                fetched_set_values,
                [] = _uncommitted_added_set_values,
                [] = _uncommitted_removed_set_values,
                casual_context_that_has_to_be_preserved
              }
            } when is_list(fetched_set_values) and is_binary(casual_context_that_has_to_be_preserved) ->
              if not (fetched_set_values |> Enum.member?(value)) do
                # Idempotent operation
                :unit
              else
                :riakc_pb_socket.update_type(
                  connection_pid,
                  {unquote(set_type_name), unquote(bucket_name)},
                  key,
                  :riakc_set.to_op(
                    {:set, fetched_set_values, [], [value], casual_context_that_has_to_be_preserved}
                  )
                )
                |> case do
                     :ok ->
                       :unit

                     {:error, riakc_pb_socket_update_type_error} ->
                       object_locator =
                         "#{unquote(bucket_name)}<#{unquote(set_type_name)}>:#{key}"
                       error_message =
                         inspect riakc_pb_socket_update_type_error
                       raise "Failed to update #{object_locator} in Riak, :riakc_pb_socket.update_type/4 responded: #{error_message}"
                   end
              end

            {:error, {:notfound, :set}} ->
              :key_not_found

            {:error, riakc_pb_socket_fetch_type_error} ->
              object_locator =
                "#{unquote(bucket_name)}<#{unquote(set_type_name)}>:#{key}"
              error_message =
                inspect riakc_pb_socket_fetch_type_error
              raise "Failed to find #{object_locator} in Riak, :riakc_pb_socket.fetch_type/3 responded: #{error_message}"
          end
        end
      end
    end
  end
end

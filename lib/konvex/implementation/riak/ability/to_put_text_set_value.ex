defmodule Konvex.Implementation.Riak.Ability.ToPutTextSetValue do
  defmacro __using__(
             [
               bucket_name: quoted_bucket_name,
               connection: quoted_riak_connection,
               set_type_name: quoted_set_type_name
             ]
           ) do
    quote do
      import Konvex.Implementation.Riak.Connection.Usage, only: [using: 2]

      @behaviour Konvex.Ability.ToPutTextSetValue

      @impl Konvex.Ability.ToPutTextSetValue
      @spec put_text_set_value(key :: String.t, value :: MapSet.t(String.t)) :: :unit
      def put_text_set_value("" = _empty_key, %MapSet{} = set) do
        raise ":riakc does not support empty keys"
      end

      def put_text_set_value(<<_, _ :: binary>> = key, empty_set) when empty_set === %MapSet{} do
        using unquote(quoted_riak_connection), fn connection_pid ->
          case :riakc_pb_socket.fetch_type(
                 connection_pid,
                 {unquote(quoted_set_type_name), unquote(quoted_bucket_name)},
                 key
               ) do
            {
              :ok,
              {
                :set,
                [] = _fetched_set_values,
                [] = _uncommitted_added_set_values,
                [] = _uncommitted_removed_set_values,
                casual_context
              }
            } when is_binary(casual_context) ->
              # Idempotent operation
              :ok

            {
              :ok,
              {
                :set,
                [<<_ :: binary>> | _] = fetched_set_values,
                [] = _uncommitted_added_set_values,
                [] = _uncommitted_removed_set_values,
                casual_context_that_has_to_be_preserved
              }
            } when is_binary(casual_context_that_has_to_be_preserved) ->
              # In this case we have to remove every already present set value
              :riakc_pb_socket.update_type(
                connection_pid,
                {unquote(quoted_set_type_name), unquote(quoted_bucket_name)},
                key,
                :riakc_set.to_op(
                  {:set, fetched_set_values, [], fetched_set_values, casual_context_that_has_to_be_preserved}
                )
              )

            {:error, {:notfound, :set}} ->
              # In this case we have to commit an empty set
              # This can't be accomplished using :riakc itself
              # (library forbids "unmodified commits", see to_op/1, update_type/5, etc.)
              # So we workaround this by creating a set with probe value and then remove it from the set
              new_set_with_probe_value =
                {:set, [], ["probe_value"], [], :undefined}
              :ok =
                :riakc_pb_socket.update_type(
                  connection_pid,
                  {unquote(quoted_set_type_name), unquote(quoted_bucket_name)},
                  key,
                  :riakc_set.to_op(new_set_with_probe_value)
                )
              {
                :ok,
                {
                  :set,
                  ["probe_value"],
                  [] = _uncommitted_added_set_values,
                  [] = _uncommitted_removed_set_values,
                  <<_ :: binary>> = casual_context_that_has_to_be_preserved
                } = persisted_new_set_with_probe_value
              } =
                :riakc_pb_socket.fetch_type(
                  connection_pid,
                  {unquote(quoted_set_type_name), unquote(quoted_bucket_name)},
                  key
                )
              empty_set =
                :riakc_set.del_element("probe_value", persisted_new_set_with_probe_value)

              :riakc_pb_socket.update_type(
                connection_pid,
                {unquote(quoted_set_type_name), unquote(quoted_bucket_name)},
                key,
                :riakc_set.to_op(empty_set)
              )

            {:error, riakc_pb_socket_fetch_type_error} ->
              object_locator =
                "#{unquote(quoted_bucket_name)}<#{unquote(quoted_set_type_name)}>:#{key}"
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

      def put_text_set_value(<<_, _ :: binary>> = key, %MapSet{} = nonempty_set) do
        using unquote(quoted_riak_connection), fn connection_pid ->
          case :riakc_pb_socket.fetch_type(
                 connection_pid,
                 {unquote(quoted_set_type_name), unquote(quoted_bucket_name)},
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
              fetched_set_values_as_map_set =
                fetched_set_values
                |> MapSet.new()
              if MapSet.equal?(nonempty_set, fetched_set_values_as_map_set) do
                # Idempotent operation
                :ok
              else
                :riakc_pb_socket.update_type(
                  connection_pid,
                  {unquote(quoted_set_type_name), unquote(quoted_bucket_name)},
                  key,
                  :riakc_set.to_op(
                    {
                      :set,
                      fetched_set_values,
                      # Add values from the new set to the fetched one that are not present in the fetched one
                      nonempty_set
                      |> MapSet.difference(fetched_set_values_as_map_set)
                      |> MapSet.to_list(),
                      # Remove values from the fetched set that are not present in the new one
                      fetched_set_values_as_map_set
                      |> MapSet.difference(nonempty_set)
                      |> MapSet.to_list(),
                      casual_context_that_has_to_be_preserved
                    }
                  )
                )
              end

            {:error, {:notfound, :set}} ->
              :riakc_pb_socket.update_type(
                connection_pid,
                {unquote(quoted_set_type_name), unquote(quoted_bucket_name)},
                key,
                :riakc_set.to_op({:set, :undefined, nonempty_set |> MapSet.to_list(), [], :undefined})
              )

            {:error, riakc_pb_socket_fetch_type_error} ->
              object_locator =
                "#{unquote(quoted_bucket_name)}<#{unquote(quoted_set_type_name)}>:#{key}"
              error_message =
                inspect riakc_pb_socket_fetch_type_error
              raise "Failed to find #{object_locator} in Riak, :riakc_pb_socket.fetch_type/3 responded: #{error_message}"
          end
          |> case do
               :ok ->
                 :unit

               {:error, riakc_pb_socket_update_type_error} ->
                 object_locator =
                   "#{unquote(quoted_bucket_name)}<#{unquote(quoted_set_type_name)}>:#{key}"
                 error_message =
                   inspect riakc_pb_socket_update_type_error
                 raise "Failed to update #{object_locator} in Riak, :riakc_pb_socket.update_type/4 responded: #{error_message}"
             end
        end
      end
    end
  end
end

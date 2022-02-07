defmodule Konvex.Implementation.Riak.Ability.ToPutTextSetValue do
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection: quoted_riak_connection,
               set_type_name: <<_, _ :: binary>> = set_type_name
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
          case Riak.find(
                 connection_pid,
                 unquote(set_type_name),
                 unquote(bucket_name),
                 key
               ) do
            {
              :set,
              [] = _fetched_set_values,
              [] = _uncommitted_added_set_values,
              [] = _uncommitted_removed_set_values,
              casual_context
            } when is_binary(casual_context) ->
              # Idempotent operation
              :ok

            {
              :set,
              fetched_set_values,
              [] = _uncommitted_added_set_values,
              [] = _uncommitted_removed_set_values,
              casual_context_to_preserve
            } = fetched_set when is_list(fetched_set_values) and is_binary(casual_context_to_preserve) ->
              # In this case we have to remove every already present set value
              Riak.update(
                connection_pid,
                {
                  :set,
                  fetched_set_values,
                  [],
                  fetched_set_values,
                  casual_context_to_preserve
                },
                unquote(set_type_name),
                unquote(bucket_name),
                key
              )

            nil ->
              # In this case we have to commit an empty set
              # This can't be accomplished using :riakc itself
              # (library forbids "unmodified commits", see to_op/1, update_type/5, etc.)
              # So we workaround this by creating a set with probe value and then remove it from the set
              with new_set_with_probe_value <-
                     {
                       :set,
                       [],
                       ["probe_value"],
                       [],
                       # No casual context yet, this is a new object
                       :undefined
                     },
                   :ok <-
                     Riak.update(
                       connection_pid,
                       new_set_with_probe_value,
                       unquote(set_type_name),
                       unquote(bucket_name),
                       key
                     ),
                   {
                     :set,
                     ["probe_value"],
                     [] = _uncommitted_added_set_values,
                     [] = _uncommitted_removed_set_values,
                     casual_context_to_preserve
                   } = persisted_new_set_with_probe_value when is_binary(casual_context_to_preserve) <-
                     Riak.find(
                       connection_pid,
                       unquote(set_type_name),
                       unquote(bucket_name),
                       key
                     ),
                   empty_set <-
                     Riak.CRDT.Set.delete(persisted_new_set_with_probe_value, "probe_value") do
                Riak.update(
                  connection_pid,
                  empty_set,
                  unquote(set_type_name),
                  unquote(bucket_name),
                  key
                )
              end

            {:error, some_reason_from_riakc_pb_socket_fetch_type} ->
              raise "Failed to find #{unquote(bucket_name)}<#{unquote(set_type_name)}>:#{key} in Riak, :riakc_pb_socket.fetch_type responded: #{inspect some_reason_from_riakc_pb_socket_fetch_type}"
          end
          |> case do
               :ok ->
                 :unit

               # Formally Riak.update/5 has three successful term more
             end
        end
      end

      def put_text_set_value(<<_, _ :: binary>> = key, %MapSet{} = nonempty_set) do
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
              casual_context_to_preserve
            } = fetched_set when is_list(fetched_set_values) and is_binary(casual_context_to_preserve) ->
              fetched_set_values_as_map_set =
                fetched_set_values
                |> MapSet.new()
              if MapSet.equal?(nonempty_set, fetched_set_values_as_map_set) do
                # Idempotent operation
                :ok
              else
                Riak.update(
                  connection_pid,
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
                    casual_context_to_preserve
                  },
                  unquote(set_type_name),
                  unquote(bucket_name),
                  key
                )
              end

            nil ->
              Riak.update(
                connection_pid,
                # Riak.CRDT.Set.new(list, context) creates {:set, list, [], [], context}
                # which maps by :riakc_set.to_op/1 to :undefined
                # which maps by :riakc_pb_socket.update_type/5 to {:error, :unmodified}
                # so to workaround this we provide {:set, [], list, [], context} instead
                {
                  :set,
                  [],
                  nonempty_set
                  |> MapSet.to_list(),
                  [],
                  # No casual context yet, this is a new object
                  :undefined
                },
                unquote(set_type_name),
                unquote(bucket_name),
                key
              )

            {:error, some_reason_from_riakc_pb_socket_fetch_type} ->
              raise "Failed to find #{unquote(bucket_name)}<#{unquote(set_type_name)}>:#{key} in Riak, :riakc_pb_socket.fetch_type responded: #{inspect some_reason_from_riakc_pb_socket_fetch_type}"
          end
          |> case do
               :ok ->
                 :unit

               # Formally Riak.update/5 has three successful term more
             end
        end
      end
    end
  end
end

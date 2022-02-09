defmodule Konvex.Implementation.Riak.Ability.ToRemoveKeyFromMapValue do
  @moduledoc """
  There is no apriori information about map_key value type in this ability,
  so to satisfy it's semantics we have to remove map_key of any value type
  as Riak treats CRDT as an additional namespace
  (so it can keep different value types under the same map_key)
  """

  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection: quoted_riak_connection,
               map_type_name: <<_, _ :: binary>> = map_type_name
             ]
           ) do
    quote do
      import Konvex.Implementation.Riak.Connection.Usage, only: [using: 2]

      @behaviour Konvex.Ability.ToRemoveKeyFromMapValue

      @impl Konvex.Ability.ToRemoveKeyFromMapValue
      @spec remove_key_from_map_value(key :: String.t, map_key :: String.t) :: :key_not_found | :unit
      def remove_key_from_map_value(key, map_key) when key === "" or map_key === "" do
        # :riakc does not support empty keys, so no way to store such
        :key_not_found
      end

      def remove_key_from_map_value(<<_, _ :: binary>> = key, <<_, _ :: binary>> = map_key) do
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
                casual_context_that_has_to_be_preserved
              }
            } when is_list(fetched_map_entries) and is_binary(casual_context_that_has_to_be_preserved) ->
              with updated_fetched_map <-
                     {
                       :map,
                       fetched_map_entries,
                       [],
                       [],
                       casual_context_that_has_to_be_preserved
                     } do
                :riakc_pb_socket.update_type(
                  connection_pid,
                  {unquote(map_type_name), unquote(bucket_name)},
                  key,
                  :riakc_map.to_op(
                    {
                      :map,
                      fetched_map_entries,
                      [],
                      # There is no apriori information about map_key value type
                      # so to satisfy semantics of the ability
                      # we have to remove each (CRDT) one
                      [:counter, :flag, :map, :register, :set]
                      |> Enum.map(fn crdt_type -> {map_key, crdt_type} end),
                      casual_context_that_has_to_be_preserved
                    }
                  )
                )
              end
              |> case do
                   :ok ->
                     :unit

                   {:error, riakc_pb_socket_update_type_error} ->
                     object_locator =
                       "#{unquote(bucket_name)}<#{unquote(map_type_name)}>:#{key}"
                     error_message =
                       inspect riakc_pb_socket_update_type_error
                     raise "Failed to update #{object_locator} in Riak, :riakc_pb_socket.update_type/4 responded: #{error_message}"
                 end

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

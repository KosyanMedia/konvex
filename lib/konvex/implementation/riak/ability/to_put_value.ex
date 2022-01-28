defmodule Konvex.Implementation.Riak.Ability.ToPutValue do
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection_provider: quoted_riak_connection_provider,
               set_type_name: <<_, _ :: binary>> = set_type_name,
               value_type: :set
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      @behaviour Konvex.Ability.ToPutSetValue

      @impl Konvex.Ability.ToPutSetValue
      @spec put(key :: String.t, value :: MapSet.t(String.t)) :: :unit
      def put(key, %MapSet{} = value) when is_binary(key) do
        connection_pid =
          Connection.Provider.get_connection_pid(unquote(quoted_riak_connection_provider))
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
            {
              :set,
              fetched_set_values,
              # Add values from the new set to the fetched one that are not present in the fetched one
              value
              |> MapSet.difference(
                   fetched_set_values
                   |> MapSet.new()
                 )
              |> MapSet.to_list(),
              # Remove values from the fetched set that are not present in the new one
              fetched_set_values
              |> MapSet.new()
              |> MapSet.difference(value)
              |> MapSet.to_list(),
              casual_context_to_preserve
            }

          nil ->
            # Riak.CRDT.Set.new(list, context) creates {:set, list, [], [], context}
            # which maps by :riakc_set.to_op/1 to :undefined
            # which maps by :riakc_pb_socket.update_type/5 to {:error, :unmodified}
            # so to workaround this we provide {:set, [], list, [], context} instead
            {
              :set,
              [],
              value
              |> MapSet.to_list(),
              [],
              # No casual context yet, this is a new object
              :undefined
            }

          {:error, some_reason_from_riakc_pb_socket_fetch_type} ->
            raise "Failed to find #{unquote(bucket_name)}<#{unquote(set_type_name)}>:#{key} in Riak, :riakc_pb_socket.fetch_type responded: #{inspect some_reason_from_riakc_pb_socket_fetch_type}"
        end
        |> (
             fn {
                  :set,
                  _set_values,
                  uncommitted_added_set_values,
                  uncommitted_removed_set_values,
                  _casual_context
                } = new_key_value_object ->
               uncommitted_added_set =
                 uncommitted_added_set_values
                 |> MapSet.new()
               uncommitted_removed_set =
                 uncommitted_removed_set_values
                 |> MapSet.new()

               # Check is intended to eliminate "both are empty lists" situation
               # (see new key creation case comment),
               # but as a bonus we eliminate other "idempotent" situations,
               # e.g. added=[1, 2], removed=[2, 1], so no need to commit anything
               case MapSet.equal?(uncommitted_added_set, uncommitted_removed_set) do
                 false ->
                   Riak.update(
                     connection_pid,
                     new_key_value_object,
                     unquote(set_type_name),
                     unquote(bucket_name),
                     key
                   )

                 true ->
                   :ok
               end
             end
             ).()
        |> case do
             :ok ->
               :unit

             # Formally it has three successful term more
           end
      end
    end
  end

  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection_provider: quoted_riak_connection_provider,
               value_type: :text
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      @behaviour Konvex.Ability.ToPutTextValue

      @impl Konvex.Ability.ToPutTextValue
      @spec put(key :: String.t, value :: String.t) :: :unit
      def put(key, value) when is_binary(key) and is_binary(value) do
        connection_pid =
          Connection.Provider.get_connection_pid(unquote(quoted_riak_connection_provider))
        case Riak.find(
               connection_pid,
               unquote(bucket_name),
               key
             ) do
          %Riak.Object{
            data: old_value,
            vclock: casual_context_to_preserve
          } = fetched_object when is_binary(old_value) and is_binary(casual_context_to_preserve) ->
            %Riak.Object{fetched_object | data: value}

          nil ->
            [bucket: unquote(bucket_name), key: key, data: value]
            |> Riak.Object.create()

          {:error, some_reason_from_riakc_pb_socket_get} ->
            raise "Failed to find #{unquote(bucket_name)}:#{key} in Riak, :riakc_pb_socket.get responded: #{inspect some_reason_from_riakc_pb_socket_get}"
        end
        |> (
             fn new_key_value_object ->
               Riak.put(
                 connection_pid,
                 new_key_value_object
               )
             end
             ).()
        |> case do
             %Riak.Object{} ->
               :unit
           end
      end
    end
  end
end

defmodule Konvex.Implementation.Riak.Ability.ToPutValue do
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection_provider: quoted_riak_connection_provider,
               map_type_name: <<_, _ :: binary>> = map_type_name,
               value_type: :map
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      @behaviour Konvex.Ability.ToPutMapValue

      @impl Konvex.Ability.ToPutMapValue
      @spec put(key :: String.t, value :: %{key :: String.t => value :: String.t}) :: :unit
      def put(<<_, _ :: binary>> = key, empty_map) when empty_map === %{} do
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
            [] = _fetched_map_entries,
            [] = _uncommitted_added_map_entries,
            [] = _uncommitted_removed_map_keys,
            casual_context
          } when is_binary(casual_context) ->
            # In this case we already have an empty map, so no further actions are needed
            :ok

          {
            :map,
            fetched_map_entries,
            [] = _uncommitted_added_map_entries,
            [] = _uncommitted_removed_map_keys,
            casual_context_to_preserve
          } = fetched_map when is_list(fetched_map_entries) and is_binary(casual_context_to_preserve) ->
            # In this case we have to remove every already present map entry
            Riak.update(
              connection_pid,
              fetched_map_entries
              |> Enum.map(
                   fn {
                        {entry_key, :register} = fetched_map_entry,
                        entry_value
                      } when is_binary(entry_key) and is_binary(entry_value) ->
                     fetched_map_entry
                   end
                 )
              |> Enum.reduce(
                   fetched_map,
                   fn fetched_map_entry, updated_map ->
                     updated_map
                     |> Riak.CRDT.Map.delete(fetched_map_entry)
                   end
                 ),
              unquote(map_type_name),
              unquote(bucket_name),
              key
            )

          nil ->
            # In this case we have to commit an empty map
            # This can't be accomplished using :riakc itself
            # (library forbids "unmodified commits", see to_op/1, update_type/5, etc.)
            # So we workaround this by creating a map with probe entry and then remove it from the map
            with new_map_with_probe_entry <-
                   Riak.CRDT.Map.new()
                   |> Riak.CRDT.Map.put(
                        "probe_key",
                        Riak.CRDT.Register.new("probe_value")
                      ),
                 :ok <-
                   Riak.update(
                     connection_pid,
                     new_map_with_probe_entry,
                     unquote(map_type_name),
                     unquote(bucket_name),
                     key
                   ),
                 {
                   :map,
                   [{{"probe_key", :register} = probe_entry_key, "probe_value"}],
                   [] = _uncommitted_added_map_entries,
                   [] = _uncommitted_removed_map_keys,
                   casual_context_to_preserve
                 } = persisted_new_map_with_probe_entry when is_binary(casual_context_to_preserve) <-
                   Riak.find(
                     connection_pid,
                     unquote(map_type_name),
                     unquote(bucket_name),
                     key
                   ),
                 empty_map <-
                   Riak.CRDT.Map.delete(
                     persisted_new_map_with_probe_entry,
                     probe_entry_key
                   ) do
              Riak.update(
                connection_pid,
                empty_map,
                unquote(map_type_name),
                unquote(bucket_name),
                key
              )
            # TODO: Much is going on in this with-block, so try to imitate transaction rollback in else-block
            end

          {:error, some_reason_from_riakc_pb_socket_fetch_type} ->
            raise "Failed to find #{unquote(bucket_name)}<#{unquote(map_type_name)}>:#{key} in Riak, :riakc_pb_socket.fetch_type responded: #{inspect some_reason_from_riakc_pb_socket_fetch_type}"
        end
        |> case do
             :ok ->
               :unit

             # Formally Riak.update/5 has three successful term more
           end
      end

      def put(<<_, _ :: binary>> = key, %{} = nonempty_map) do
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
            casual_context_to_preserve
          } = fetched_map when is_list(fetched_map_entries) and is_binary(casual_context_to_preserve) ->
            new_map_keys =
              nonempty_map
              |> Map.keys()
              |> MapSet.new()
            fetched_map_keys =
              fetched_map_entries
              |> Enum.map(
                   fn {
                        {entry_key, :register},
                        entry_value
                      } when is_binary(entry_key) and is_binary(entry_value) ->
                     entry_key
                   end
                 )
              |> MapSet.new()
            fetched_map_keys
            |> MapSet.difference(new_map_keys)
            |> Enum.reduce(
                 fetched_map,
                 fn fetched_map_key_that_is_not_present_in_the_new_one,
                    map_without_redundant_keys when
                      is_binary(fetched_map_key_that_is_not_present_in_the_new_one) ->
                   map_without_redundant_keys
                   |> Riak.CRDT.Map.delete(
                        {
                          fetched_map_key_that_is_not_present_in_the_new_one,
                          :register
                        }
                      )
                 end
               )

          nil ->
            Riak.CRDT.Map.new()

          {:error, some_reason_from_riakc_pb_socket_fetch_type} ->
            raise "Failed to find #{unquote(bucket_name)}<#{unquote(map_type_name)}>:#{key} in Riak, :riakc_pb_socket.fetch_type responded: #{inspect some_reason_from_riakc_pb_socket_fetch_type}"
        end
        |> (
             fn map ->
               nonempty_map
               |> Enum.reduce(
                    map,
                    fn {entry_key, entry_value},
                       map_extended_with_new_map_entries when
                         is_binary(entry_key)
                         and is_binary(entry_value) ->
                      map_extended_with_new_map_entries
                      |> Riak.CRDT.Map.update(
                           :register,
                           entry_key,
                           fn {:register, old_value, :undefined} = entry_value_riak_representation when
                                is_binary(old_value) ->
                             entry_value_riak_representation
                             |> Riak.CRDT.Register.set(entry_value)
                           end
                         )
                    end
                  )
             end
             ).()
        |> (
             fn new_map ->
               Riak.update(
                 connection_pid,
                 new_map,
                 unquote(map_type_name),
                 unquote(bucket_name),
                 key
               )
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
               set_type_name: <<_, _ :: binary>> = set_type_name,
               value_type: :set
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      @behaviour Konvex.Ability.ToPutSetValue

      @impl Konvex.Ability.ToPutSetValue
      @spec put(key :: String.t, value :: MapSet.t(String.t)) :: :unit
      def put(<<_, _ :: binary>> = key, empty_set) when empty_set === %MapSet{} do
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
            [] = _fetched_set_values,
            [] = _uncommitted_added_set_values,
            [] = _uncommitted_removed_set_values,
            _casual_context
          } ->
            # In this case we already have an empty set, so no further actions are needed
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
              # TODO: Much is going on in this with-block, so try to imitate transaction rollback in else-block
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

      def put(<<_, _ :: binary>> = key, %MapSet{} = nonempty_set) do
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
      def put(<<_, _ :: binary>> = key, value) when is_binary(value) do
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

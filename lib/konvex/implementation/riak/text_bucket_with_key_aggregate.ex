defmodule Konvex.Implementation.Riak.TextBucketWithKeyAggregate do
  @doc """
  Regular Riak bucket with text values CRUD-client extended with get-all-keys ability
  implemented using two-bucket-setup (second one is used for key aggregation of the first one)
  """
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection_provider: quoted_riak_connection_provider,
               key_aggregate_bucket_name: <<_, _ :: binary>> = key_aggregate_bucket_name,
               key_aggregate_bucket_key: <<_, _ :: binary>> = key_aggregate_bucket_key,
               set_type_name: <<_, _ :: binary>> = set_type_name
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      defmodule Private.Implementation.Ability.ToAddSetValue do
        use Konvex.Implementation.Riak.Ability.ToAddSetValue,
            bucket_name: unquote(key_aggregate_bucket_name),
            connection_provider: unquote(quoted_riak_connection_provider),
            set_type_name: unquote(set_type_name)
      end

      defmodule Private.Implementation.Ability.ToDeleteKey do
        use Konvex.Implementation.Riak.Ability.ToDeleteKey,
            bucket_name: unquote(bucket_name),
            connection_provider: unquote(quoted_riak_connection_provider),
            value_type: :text
      end

      defmodule Private.Implementation.Ability.ToPutValue do
        use Konvex.Implementation.Riak.Ability.ToPutValue,
            bucket_name: unquote(bucket_name),
            connection_provider: unquote(quoted_riak_connection_provider),
            value_type: :text
      end

      defmodule Private.Implementation.Ability.ToRemoveSetValue do
        use Konvex.Implementation.Riak.Ability.ToRemoveSetValue,
            bucket_name: unquote(key_aggregate_bucket_name),
            connection_provider: unquote(quoted_riak_connection_provider),
            set_type_name: unquote(set_type_name)
      end

      @behaviour Konvex.Ability.ToDeleteKey
      @behaviour Konvex.Ability.ToPutTextValue

      use Konvex.Implementation.Riak.Ability.ToCheckKeyExistence,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          value_type: :text
      use Konvex.Implementation.Riak.Ability.ToGetAllKeys,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          key_aggregate_bucket_name: unquote(key_aggregate_bucket_name),
          key_aggregate_bucket_key: unquote(key_aggregate_bucket_key),
          set_type_name: unquote(set_type_name)
      use Konvex.Implementation.Riak.Ability.ToGetValue,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          value_type: :text

      @impl Konvex.Ability.ToDeleteKey
      @spec delete(key :: String.t) :: :unit
      def delete(key) when is_binary(key) do
        ability_implementation_to_delete_key_module =
          Module.concat(__MODULE__, Private.Implementation.Ability.ToDeleteKey)
        ability_implementation_to_remove_set_value_module =
          Module.concat(__MODULE__, Private.Implementation.Ability.ToRemoveSetValue)
        with :unit <-
               ability_implementation_to_delete_key_module.delete(key),
             :unit <-
               ability_implementation_to_remove_set_value_module.remove(
                 unquote(key_aggregate_bucket_key),
                 key
               ) do
          :unit
        end
      end

      @impl Konvex.Ability.ToPutTextValue
      @spec put(key :: String.t, value :: String.t) :: :unit
      def put(key, value) when is_binary(key) and is_binary(value) do
        ability_implementation_to_add_set_value_module =
          Module.concat(__MODULE__, Private.Implementation.Ability.ToAddSetValue)
        ability_implementation_to_put_text_value_module =
          Module.concat(__MODULE__, Private.Implementation.Ability.ToPutValue)
        with :unit <-
               ability_implementation_to_put_text_value_module.put(key, value),
             :unit <-
               ability_implementation_to_add_set_value_module.add(
                 unquote(key_aggregate_bucket_key),
                 key
               ) do
          :unit
        end
      end
    end
  end
end

defmodule Konvex.Implementation.Riak.TextBucketWithKeyValueAggregate do
  @doc """
  Regular Riak bucket with text values CRUD-client extended with
  both get-all-keys and get-all-key-values abilities (get-all-keys is derived from get-all-key-values)
  implemented using two-bucket-setup (second one is used for key-value pair aggregation of the first one)
  """
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection_provider: quoted_riak_connection_provider,
               key_value_aggregate_bucket_name: <<_, _ :: binary>> = key_value_aggregate_bucket_name,
               key_value_aggregate_bucket_key: <<_, _ :: binary>> = key_value_aggregate_bucket_key,
               map_type_name: <<_, _ :: binary>> = map_type_name
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      defmodule Private.Implementation.Ability.ToDeleteKey do
        use Konvex.Implementation.Riak.Ability.ToDeleteKey,
            bucket_name: unquote(bucket_name),
            connection_provider: unquote(quoted_riak_connection_provider),
            value_type: :text
      end

      defmodule Private.Implementation.Ability.ToPutTextValue do
        use Konvex.Implementation.Riak.Ability.ToPutValue,
            bucket_name: unquote(bucket_name),
            connection_provider: unquote(quoted_riak_connection_provider),
            value_type: :text
      end

      defmodule Private.Implementation.Ability.ToRemoveMapKey do
        use Konvex.Implementation.Riak.Ability.ToRemoveMapKey,
            bucket_name: unquote(key_value_aggregate_bucket_name),
            connection_provider: unquote(quoted_riak_connection_provider),
            map_type_name: unquote(map_type_name)
      end

      defmodule Private.Implementation.Ability.ToUpdateMapValue do
        use Konvex.Implementation.Riak.Ability.ToUpdateMapValue,
            bucket_name: unquote(key_value_aggregate_bucket_name),
            connection_provider: unquote(quoted_riak_connection_provider),
            map_type_name: unquote(map_type_name)
      end

      @behaviour Konvex.Ability.ToDeleteKey
      @behaviour Konvex.Ability.ToGetAllKeys
      @behaviour Konvex.Ability.ToPutTextValue

      use Konvex.Implementation.Riak.Ability.ToCheckKeyExistence,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          value_type: :text
      use Konvex.Implementation.Riak.Ability.ToGetAllKeyValues,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          key_value_aggregate_bucket_name: unquote(key_value_aggregate_bucket_name),
          key_value_aggregate_bucket_key: unquote(key_value_aggregate_bucket_key),
          map_type_name: unquote(map_type_name)
      use Konvex.Implementation.Riak.Ability.ToGetValue,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          value_type: :text

      @impl Konvex.Ability.ToDeleteKey
      @spec delete(key :: String.t) :: :unit
      def delete(key) when is_binary(key) do
        with :unit <-
               Private.Implementation.Ability.ToDeleteKey.delete(key),
             :unit <-
               Private.Implementation.Ability.ToRemoveMapKey.remove(unquote(key_value_aggregate_bucket_key), key) do
          :unit
        end
      end

      @impl Konvex.Ability.ToGetAllKeys
      @spec get_all_keys() :: MapSet.t(key :: String.t)
      def get_all_keys() do
        get_all_key_values()
        |> Map.keys()
      end

      @impl Konvex.Ability.ToPutTextValue
      @spec put(key :: String.t, value :: String.t) :: :unit
      def put(key, value) when is_binary(key) and is_binary(value) do
        with :unit <-
               Private.Implementation.Ability.ToPutTextValue.put(key, value),
             :unit <-
               Private.Implementation.Ability.ToUpdateMapValue.update(
                 unquote(key_value_aggregate_bucket_key),
                 key,
                 fn _old_value -> value end
               ) do
          :unit
        end
      end
    end
  end
end

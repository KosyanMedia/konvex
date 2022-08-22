defmodule Konvex.Implementation.Riak.TextBucketWithKeyValueAggregate do
  @doc """
  Regular Riak bucket with text values CRUD-client extended with
  both get-all-keys and get-all-key-values abilities (get-all-keys is derived from get-all-key-values)
  implemented using two-bucket-setup (second one is used for key-value pair aggregation of the first one)
  """
  defmacro __using__(
             [
               bucket_name: quoted_bucket_name,
               conflict_resolution_strategy_module: quoted_conflict_resolution_strategy_module,
               connection: quoted_riak_connection,
               key_value_aggregate_bucket_name: quoted_key_value_aggregate_bucket_name,
               key_value_aggregate_bucket_key: quoted_key_value_aggregate_bucket_key,
               map_type_name: quoted_map_type_name
             ]
           ) do
    quote do
      @behaviour Konvex.Ability.ToDeleteKey
      @behaviour Konvex.Ability.ToGetAllKeys
      @behaviour Konvex.Ability.ToPutTextValue

      use Konvex.Implementation.Riak.Ability.ToCheckKeyExists,
          bucket_name: unquote(quoted_bucket_name),
          connection: unquote(quoted_riak_connection),
          value_type: :text
      use Konvex.Implementation.Riak.Ability.ToGetAllTextKeyValues,
          bucket_name: unquote(quoted_bucket_name),
          connection: unquote(quoted_riak_connection),
          key_value_aggregate_bucket_name: unquote(quoted_key_value_aggregate_bucket_name),
          key_value_aggregate_bucket_key: unquote(quoted_key_value_aggregate_bucket_key),
          map_type_name: unquote(quoted_map_type_name)
      use Konvex.Implementation.Riak.Ability.ToGetTextValue,
          bucket_name: unquote(quoted_bucket_name),
          conflict_resolution_strategy_module: unquote(quoted_conflict_resolution_strategy_module),
          connection: unquote(quoted_riak_connection)

      defmodule Private.Implementation.Ability.ToDeleteKey do
        use Konvex.Implementation.Riak.Ability.ToDeleteKey,
            bucket_name: unquote(quoted_bucket_name),
            connection: unquote(quoted_riak_connection),
            value_type: :text
      end

      defmodule Private.Implementation.Ability.ToPutTextValue do
        use Konvex.Implementation.Riak.Ability.ToPutTextValue,
            bucket_name: unquote(quoted_bucket_name),
            connection: unquote(quoted_riak_connection)
      end

      defmodule Private.Implementation.Ability.ToPutValueToTextMapValue do
        use Konvex.Implementation.Riak.Ability.ToPutValueToTextMapValue,
            bucket_name: unquote(quoted_key_value_aggregate_bucket_name),
            connection: unquote(quoted_riak_connection),
            map_type_name: unquote(quoted_map_type_name)
      end

      defmodule Private.Implementation.Ability.ToRemoveKeyFromMapValue do
        use Konvex.Implementation.Riak.Ability.ToRemoveKeyFromMapValue,
            bucket_name: unquote(quoted_key_value_aggregate_bucket_name),
            connection: unquote(quoted_riak_connection),
            map_type_name: unquote(quoted_map_type_name)
      end

      @impl Konvex.Ability.ToDeleteKey
      @spec delete_key(key :: String.t) :: :unit
      def delete_key(key) when is_binary(key) do
        :unit =
          Private.Implementation.Ability.ToDeleteKey.delete_key(key)
        :unit =
          Private.Implementation.Ability.ToRemoveKeyFromMapValue.remove_key_from_map_value(
            unquote(quoted_key_value_aggregate_bucket_key),
            key
          )
      end

      @impl Konvex.Ability.ToGetAllKeys
      @spec get_all_keys() :: MapSet.t(key :: String.t)
      def get_all_keys() do
        get_all_text_key_values()
        |> Map.keys()
      end

      @impl Konvex.Ability.ToPutTextValue
      @spec put_text_value(key :: String.t, value :: String.t) :: :unit
      def put_text_value(key, value) when is_binary(key) and is_binary(value) do
        :unit =
          Private.Implementation.Ability.ToPutTextValue.put_text_value(key, value)
        :unit =
          Private.Implementation.Ability.ToPutValueToTextMapValue.put_value_to_text_map_value(
            unquote(quoted_key_value_aggregate_bucket_key),
            key,
            value
          )
      end
    end
  end
end

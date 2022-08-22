defmodule Konvex.Implementation.Riak.TextBucketWithKeyAggregate do
  @doc """
  Regular Riak bucket with text values CRUD-client extended with get-all-keys ability
  implemented using two-bucket-setup (second one is used for key aggregation of the first one)
  """
  defmacro __using__(
             [
               bucket_name: quoted_bucket_name,
               conflict_resolution_strategy_module: quoted_conflict_resolution_strategy_module,
               connection: quoted_riak_connection,
               key_aggregate_bucket_name: quoted_key_aggregate_bucket_name,
               key_aggregate_bucket_key: quoted_key_aggregate_bucket_key,
               set_type_name: quoted_set_type_name
             ]
           ) do
    quote do
      @behaviour Konvex.Ability.ToDeleteKey
      @behaviour Konvex.Ability.ToPutTextValue

      use Konvex.Implementation.Riak.Ability.ToCheckKeyExists,
          bucket_name: unquote(quoted_bucket_name),
          connection: unquote(quoted_riak_connection),
          value_type: :text
      use Konvex.Implementation.Riak.Ability.ToGetAllKeys,
          bucket_name: unquote(quoted_bucket_name),
          connection: unquote(quoted_riak_connection),
          key_aggregate_bucket_name: unquote(quoted_key_aggregate_bucket_name),
          key_aggregate_bucket_key: unquote(quoted_key_aggregate_bucket_key),
          set_type_name: unquote(quoted_set_type_name)
      use Konvex.Implementation.Riak.Ability.ToGetTextValue,
          bucket_name: unquote(quoted_bucket_name),
          conflict_resolution_strategy_module: unquote(quoted_conflict_resolution_strategy_module),
          connection: unquote(quoted_riak_connection)

      defmodule Private.Implementation.Ability.ToAddValueToTextSetValue do
        use Konvex.Implementation.Riak.Ability.ToAddValueToTextSetValue,
            bucket_name: unquote(quoted_key_aggregate_bucket_name),
            connection: unquote(quoted_riak_connection),
            set_type_name: unquote(quoted_set_type_name)
      end

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

      defmodule Private.Implementation.Ability.ToRemoveValueFromTextSetValue do
        use Konvex.Implementation.Riak.Ability.ToRemoveValueFromTextSetValue,
            bucket_name: unquote(quoted_key_aggregate_bucket_name),
            connection: unquote(quoted_riak_connection),
            set_type_name: unquote(quoted_set_type_name)
      end

      @impl Konvex.Ability.ToDeleteKey
      @spec delete_key(key :: String.t) :: :unit
      def delete_key(key) when is_binary(key) do
        :unit =
          Private.Implementation.Ability.ToDeleteKey.delete_key(key)
        :unit =
          Private.Implementation.Ability.ToRemoveValueFromTextSetValue.remove_value_from_text_set_value(
            unquote(quoted_key_aggregate_bucket_key),
            key
          )
      end

      @impl Konvex.Ability.ToPutTextValue
      @spec put_text_value(key :: String.t, value :: String.t) :: :unit
      def put_text_value(key, value) when is_binary(key) and is_binary(value) do
        :unit =
          Private.Implementation.Ability.ToPutTextValue.put_text_value(key, value)
        :unit =
          Private.Implementation.Ability.ToAddValueToTextSetValue.add_value_to_text_set_value(
            unquote(quoted_key_aggregate_bucket_key),
            key
          )
      end
    end
  end
end

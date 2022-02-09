defmodule Konvex.Implementation.Riak.AnyBucket do
  @doc """
  Regular Riak bucket with any values (stored as binaries) CRUD-client
  """
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               conflict_resolution_strategy_module: conflict_resolution_strategy_module,
               connection: quoted_riak_connection
             ]
           ) do
    quote do
      use Konvex.Implementation.Riak.Ability.ToCheckKeyExists,
          bucket_name: unquote(bucket_name),
          connection: unquote(quoted_riak_connection),
          value_type: :text
      use Konvex.Implementation.Riak.Ability.ToDeleteKey,
          bucket_name: unquote(bucket_name),
          connection: unquote(quoted_riak_connection),
          value_type: :text
      use Konvex.Implementation.Riak.Ability.ToGetAnyValue,
          bucket_name: unquote(bucket_name),
          conflict_resolution_strategy_module: unquote(conflict_resolution_strategy_module),
          connection: unquote(quoted_riak_connection)
      use Konvex.Implementation.Riak.Ability.ToPutAnyValue,
          bucket_name: unquote(bucket_name),
          connection: unquote(quoted_riak_connection)
    end
  end
end

defmodule Konvex.Implementation.Riak.AnyBucket do
  @doc """
  Regular Riak bucket with any values (stored as binaries) CRUD-client
  """
  defmacro __using__(
             [
               bucket_name: quoted_bucket_name,
               conflict_resolution_strategy_module: quoted_conflict_resolution_strategy_module,
               connection: quoted_riak_connection
             ]
           ) do
    quote do
      use Konvex.Implementation.Riak.Ability.ToCheckKeyExists,
          bucket_name: unquote(quoted_bucket_name),
          connection: unquote(quoted_riak_connection),
          value_type: :text
      use Konvex.Implementation.Riak.Ability.ToDeleteKey,
          bucket_name: unquote(quoted_bucket_name),
          connection: unquote(quoted_riak_connection),
          value_type: :text
      use Konvex.Implementation.Riak.Ability.ToGetAnyValue,
          bucket_name: unquote(quoted_bucket_name),
          conflict_resolution_strategy_module: unquote(quoted_conflict_resolution_strategy_module),
          connection: unquote(quoted_riak_connection)
      use Konvex.Implementation.Riak.Ability.ToPutAnyValue,
          bucket_name: unquote(quoted_bucket_name),
          connection: unquote(quoted_riak_connection)
    end
  end
end

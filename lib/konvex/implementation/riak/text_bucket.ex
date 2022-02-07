defmodule Konvex.Implementation.Riak.TextBucket do
  @doc """
  Regular Riak bucket with text values CRUD-client
  """
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               conflict_resolution_strategy_module: conflict_resolution_strategy_module,
               connection_provider: quoted_riak_connection_provider
             ]
           ) do
    quote do
      use Konvex.Implementation.Riak.Ability.ToCheckKeyExists,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          value_type: :text
      use Konvex.Implementation.Riak.Ability.ToDeleteKey,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          value_type: :text
      use Konvex.Implementation.Riak.Ability.ToGetTextValue,
          bucket_name: unquote(bucket_name),
          conflict_resolution_strategy_module: unquote(conflict_resolution_strategy_module),
          connection_provider: unquote(quoted_riak_connection_provider)
      use Konvex.Implementation.Riak.Ability.ToPutTextValue,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider)
    end
  end
end

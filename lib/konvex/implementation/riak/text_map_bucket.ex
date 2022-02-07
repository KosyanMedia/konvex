defmodule Konvex.Implementation.Riak.TextMapBucket do
  @doc """
  Riak bucket with map (CRDT) values CRUD-client
  """
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection_provider: quoted_riak_connection_provider,
               map_type_name: <<_, _ :: binary>> = map_type_name
             ]
           ) do
    quote do
      use Konvex.Implementation.Riak.Ability.ToCheckKeyExists,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          crdt_name: unquote(map_type_name),
          value_type: :crdt
      use Konvex.Implementation.Riak.Ability.ToDeleteKey,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          crdt_name: unquote(map_type_name),
          value_type: :crdt
      use Konvex.Implementation.Riak.Ability.ToGetTextMapValue,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          map_type_name: unquote(map_type_name)
      use Konvex.Implementation.Riak.Ability.ToPutTextMapValue,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          map_type_name: unquote(map_type_name)
      use Konvex.Implementation.Riak.Ability.ToPutValueToTextMapValue,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          map_type_name: unquote(map_type_name)
      use Konvex.Implementation.Riak.Ability.ToRemoveKeyFromMapValue,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          map_type_name: unquote(map_type_name)
    end
  end
end

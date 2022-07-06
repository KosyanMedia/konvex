defmodule Konvex.Implementation.Riak.TextMapBucket do
  @doc """
  Riak bucket with map (CRDT) values CRUD-client
  """
  defmacro __using__(
             [
               bucket_name: quoted_bucket_name,
               connection: quoted_riak_connection,
               map_type_name: quoted_map_type_name
             ]
           ) do
    quote do
      use Konvex.Implementation.Riak.Ability.ToCheckKeyExists,
          bucket_name: unquote(quoted_bucket_name),
          connection: unquote(quoted_riak_connection),
          crdt_name: unquote(quoted_map_type_name),
          value_type: :crdt
      use Konvex.Implementation.Riak.Ability.ToDeleteKey,
          bucket_name: unquote(quoted_bucket_name),
          connection: unquote(quoted_riak_connection),
          crdt_name: unquote(quoted_map_type_name),
          value_type: :crdt
      use Konvex.Implementation.Riak.Ability.ToGetTextMapValue,
          bucket_name: unquote(quoted_bucket_name),
          connection: unquote(quoted_riak_connection),
          map_type_name: unquote(quoted_map_type_name)
      use Konvex.Implementation.Riak.Ability.ToPutTextMapValue,
          bucket_name: unquote(quoted_bucket_name),
          connection: unquote(quoted_riak_connection),
          map_type_name: unquote(quoted_map_type_name)
      use Konvex.Implementation.Riak.Ability.ToPutValueToTextMapValue,
          bucket_name: unquote(quoted_bucket_name),
          connection: unquote(quoted_riak_connection),
          map_type_name: unquote(quoted_map_type_name)
      use Konvex.Implementation.Riak.Ability.ToRemoveKeyFromMapValue,
          bucket_name: unquote(quoted_bucket_name),
          connection: unquote(quoted_riak_connection),
          map_type_name: unquote(quoted_map_type_name)
    end
  end
end

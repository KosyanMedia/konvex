defmodule Konvex.Implementation.Riak.TextSetBucket do
  @doc """
  Riak bucket with set (CRDT) values CRUD-client
  """
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection: quoted_riak_connection,
               set_type_name: <<_, _ :: binary>> = set_type_name
             ]
           ) do
    quote do
      use Konvex.Implementation.Riak.Ability.ToAddValueToTextSetValue,
          bucket_name: unquote(bucket_name),
          connection: unquote(quoted_riak_connection),
          set_type_name: unquote(set_type_name)
      use Konvex.Implementation.Riak.Ability.ToCheckKeyExists,
          bucket_name: unquote(bucket_name),
          connection: unquote(quoted_riak_connection),
          crdt_name: unquote(set_type_name),
          value_type: :crdt
      use Konvex.Implementation.Riak.Ability.ToDeleteKey,
          bucket_name: unquote(bucket_name),
          connection: unquote(quoted_riak_connection),
          crdt_name: unquote(set_type_name),
          value_type: :crdt
      use Konvex.Implementation.Riak.Ability.ToGetTextSetValue,
          bucket_name: unquote(bucket_name),
          connection: unquote(quoted_riak_connection),
          set_type_name: unquote(set_type_name)
      use Konvex.Implementation.Riak.Ability.ToPutTextSetValue,
          bucket_name: unquote(bucket_name),
          connection: unquote(quoted_riak_connection),
          set_type_name: unquote(set_type_name)
      use Konvex.Implementation.Riak.Ability.ToRemoveValueFromTextSetValue,
          bucket_name: unquote(bucket_name),
          connection: unquote(quoted_riak_connection),
          set_type_name: unquote(set_type_name)
    end
  end
end

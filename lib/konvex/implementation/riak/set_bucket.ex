defmodule Konvex.Implementation.Riak.SetBucket do
  @doc """
  Riak bucket with set (CRDT) values CRUD-client
  """
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection_provider: quoted_riak_connection_provider,
               set_type_name: <<_, _ :: binary>> = set_type_name
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      use Konvex.Implementation.Riak.Ability.ToAddSetValue,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          set_type_name: unquote(set_type_name)
      use Konvex.Implementation.Riak.Ability.ToCheckKeyExistence,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          crdt_name: unquote(set_type_name),
          value_type: :crdt
      use Konvex.Implementation.Riak.Ability.ToDeleteKey,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          crdt_name: unquote(set_type_name),
          value_type: :crdt
      use Konvex.Implementation.Riak.Ability.ToGetValue,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          set_type_name: unquote(set_type_name),
          value_type: :set
      use Konvex.Implementation.Riak.Ability.ToPutValue,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          set_type_name: unquote(set_type_name),
          value_type: :set
      use Konvex.Implementation.Riak.Ability.ToRemoveSetValue,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          set_type_name: unquote(set_type_name)
    end
  end
end

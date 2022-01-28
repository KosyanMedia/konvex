defmodule Konvex.Implementation.Riak.TextBucket do
  @doc """
  Regular Riak bucket with text values CRUD-client
  """
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection_provider: quoted_riak_connection_provider
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      use Konvex.Implementation.Riak.Ability.ToCheckKeyExistence,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          value_type: :text
      use Konvex.Implementation.Riak.Ability.ToDeleteKey,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          value_type: :text
      use Konvex.Implementation.Riak.Ability.ToGetValue,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          value_type: :text
      use Konvex.Implementation.Riak.Ability.ToPutValue,
          bucket_name: unquote(bucket_name),
          connection_provider: unquote(quoted_riak_connection_provider),
          value_type: :text
    end
  end
end

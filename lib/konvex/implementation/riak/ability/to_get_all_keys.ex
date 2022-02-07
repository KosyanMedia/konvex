defmodule Konvex.Implementation.Riak.Ability.ToGetAllKeys do
  @doc """
  Bucket name specification is compulsory
  to be explicit about two-bucket-setup for Riak implementation
  """
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = _bucket_name,
               connection_provider: quoted_riak_connection_provider,
               key_aggregate_bucket_name: <<_, _ :: binary>> = key_aggregate_bucket_name,
               key_aggregate_bucket_key: <<_, _ :: binary>> = key_aggregate_bucket_key,
               set_type_name: <<_, _ :: binary>> = set_type_name
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      defmodule Private.Implementation.Ability.ToGetTextSetValue do
        use Konvex.Implementation.Riak.Ability.ToGetTextSetValue,
            bucket_name: unquote(key_aggregate_bucket_name),
            connection_provider: unquote(quoted_riak_connection_provider),
            set_type_name: unquote(set_type_name)
      end

      @behaviour Konvex.Ability.ToGetAllKeys

      @impl Konvex.Ability.ToGetAllKeys
      @spec get_all_keys() :: MapSet.t(key :: String.t)
      def get_all_keys() do
        case Private.Implementation.Ability.ToGetTextSetValue
             .get_text_set_value(unquote(key_aggregate_bucket_key)) do
          :key_not_found ->
            raise "To query all keys you have to set up storage aggregate: "
                  <> "#{unquote(key_aggregate_bucket_name)}"
                  <> "<#{unquote(set_type_name)}>"
                     <> ":#{unquote(key_aggregate_bucket_key)}"

          %MapSet{} = all_keys ->
            all_keys
        end
      end
    end
  end
end

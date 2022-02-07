defmodule Konvex.Implementation.Riak.Ability.ToGetAllTextKeyValues do
  @doc """
  Bucket name specification is compulsory
  to be explicit about two-bucket-setup for Riak implementation
  """
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = _bucket_name,
               connection_provider: quoted_riak_connection_provider,
               key_value_aggregate_bucket_name: <<_, _ :: binary>> = key_value_aggregate_bucket_name,
               key_value_aggregate_bucket_key: <<_, _ :: binary>> = key_value_aggregate_bucket_key,
               map_type_name: <<_, _ :: binary>> = map_type_name
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      defmodule Private.Implementation.Ability.ToGetTextMapValue do
        use Konvex.Implementation.Riak.Ability.ToGetTextMapValue,
            bucket_name: unquote(key_value_aggregate_bucket_name),
            connection_provider: unquote(quoted_riak_connection_provider),
            map_type_name: unquote(map_type_name)
      end

      @behaviour Konvex.Ability.ToGetAllTextKeyValues

      @impl Konvex.Ability.ToGetAllTextKeyValues
      @spec get_all_text_key_values() :: %{key :: String.t => value :: String.t}
      def get_all_text_key_values() do
        case Private.Implementation.Ability.ToGetTextMapValue
             .get_text_map_value(unquote(key_value_aggregate_bucket_key)) do
          :key_not_found ->
            raise "To query all text key values you have to set up storage aggregate: "
                  <> "#{unquote(key_value_aggregate_bucket_name)}"
                  <> "<#{unquote(map_type_name)}>"
                     <> ":#{unquote(key_value_aggregate_bucket_key)}"

          %{} = all_text_key_values ->
            all_text_key_values
        end
      end
    end
  end
end
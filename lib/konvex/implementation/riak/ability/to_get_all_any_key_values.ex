defmodule Konvex.Implementation.Riak.Ability.ToGetAllAnyKeyValues do
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection: quoted_riak_connection,
               key_value_aggregate_bucket_name: <<_, _ :: binary>> = key_value_aggregate_bucket_name,
               key_value_aggregate_bucket_key: <<_, _ :: binary>> = key_value_aggregate_bucket_key,
               map_type_name: <<_, _ :: binary>> = map_type_name
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      defmodule Private.Implementation.Ability.ToGetAllTextKeyValues do
        use Konvex.Implementation.Riak.Ability.ToGetAllTextKeyValues,
            bucket_name: unquote(bucket_name),
            connection: unquote(quoted_riak_connection),
            key_value_aggregate_bucket_name: unquote(key_value_aggregate_bucket_name),
            key_value_aggregate_bucket_key: unquote(key_value_aggregate_bucket_key),
            map_type_name: unquote(map_type_name)
      end

      @behaviour Konvex.Ability.ToGetAllAnyKeyValues

      @impl Konvex.Ability.ToGetAllAnyKeyValues
      @spec get_all_any_key_values() :: %{key :: String.t => value :: any}
      def get_all_any_key_values() do
        Private.Implementation.Ability.ToGetAllTextKeyValues.get_all_text_key_values()
        |> Enum.map(
             fn {key, binary_value} when is_binary(key) and is_binary(binary_value) ->
               {key, :erlang.binary_to_term(binary_value)}
             end
           )
        |> Map.new()
      end
    end
  end
end

defmodule Konvex.Implementation.Riak.Ability.ToGetAnyValue do
  defmacro __using__(
             [
               bucket_name: quoted_bucket_name,
               conflict_resolution_strategy_module: quoted_conflict_resolution_strategy_module,
               connection: quoted_riak_connection
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      defmodule Private.Implementation.Ability.ToGetTextValue do
        use Konvex.Implementation.Riak.Ability.ToGetTextValue,
            bucket_name: unquote(quoted_bucket_name),
            conflict_resolution_strategy_module: unquote(quoted_conflict_resolution_strategy_module),
            connection: unquote(quoted_riak_connection)
      end

      @behaviour Konvex.Ability.ToGetAnyValue

      @impl Konvex.Ability.ToGetAnyValue
      @spec get_any_value(key :: String.t) :: :key_not_found | any
      def get_any_value(key) when is_binary(key) do
        with binary_value when is_binary(binary_value)
             <- Private.Implementation.Ability.ToGetTextValue.get_text_value(key) do
          :erlang.binary_to_term(binary_value)
        end
      end
    end
  end
end

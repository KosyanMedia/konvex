defmodule Konvex.Implementation.Riak.Ability.ToPutAnyValue do
  defmacro __using__(
             [
               bucket_name: quoted_bucket_name,
               connection: quoted_riak_connection
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      defmodule Private.Implementation.Ability.ToPutTextValue do
        use Konvex.Implementation.Riak.Ability.ToPutTextValue,
            bucket_name: unquote(quoted_bucket_name),
            connection: unquote(quoted_riak_connection)
      end

      @behaviour Konvex.Ability.ToPutAnyValue

      @impl Konvex.Ability.ToPutAnyValue
      @spec put_any_value(key :: String.t, value :: any) :: :unit
      def put_any_value(key, value) when is_binary(key) do
        Private.Implementation.Ability.ToPutTextValue.put_text_value(key, :erlang.term_to_binary(value))
      end
    end
  end
end

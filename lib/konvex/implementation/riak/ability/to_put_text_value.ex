defmodule Konvex.Implementation.Riak.Ability.ToPutTextValue do
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection: quoted_riak_connection
             ]
           ) do
    quote do
      import Konvex.Implementation.Riak.Connection.Usage, only: [using: 2]

      @behaviour Konvex.Ability.ToPutTextValue

      @impl Konvex.Ability.ToPutTextValue
      @spec put_text_value(key :: String.t, value :: String.t) :: :unit
      def put_text_value("" = _empty_key, value) when is_binary(value) do
        raise ":riakc does not support empty keys"
      end

      def put_text_value(<<_, _ :: binary>> = key, value) when is_binary(value) do
        using unquote(quoted_riak_connection), fn connection_pid ->
          case Riak.find(
                 connection_pid,
                 unquote(bucket_name),
                 key
               ) do
            %Riak.Object{
              data: old_value,
              vclock: casual_context_to_preserve
            } = fetched_object when is_binary(old_value) and is_binary(casual_context_to_preserve) ->
              %Riak.Object{fetched_object | data: value}

            nil ->
              [bucket: unquote(bucket_name), key: key, data: value]
              |> Riak.Object.create()

            {:error, some_reason_from_riakc_pb_socket_get} ->
              raise "Failed to find #{unquote(bucket_name)}:#{key} in Riak, :riakc_pb_socket.get responded: #{inspect some_reason_from_riakc_pb_socket_get}"
          end
          |> (
               fn %Riak.Object{} = new_key_value_object ->
                 Riak.put(connection_pid, new_key_value_object)
               end
               ).()
          |> case do
               %Riak.Object{} ->
                 :unit
             end
        end
      end
    end
  end
end

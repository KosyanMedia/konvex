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
          case :riakc_pb_socket.get(connection_pid, unquote(bucket_name), key) do
            {
              :ok,
              {
                :riakc_obj,
                unquote(bucket_name),
                object_key,
                casual_context_that_has_to_be_preserved,
                # Can be the only one or a list of conflicting sibling values
                # (which would be dropped by committing a new value)
                object_values,
                # Uncommitted new value metadata as Erlang dict
                :undefined,
                # Uncommitted new value
                :undefined
              } = riak_obj
            } when object_key === key and is_binary(casual_context_that_has_to_be_preserved) and is_list(object_values) ->
              :riakc_obj.update_value(riak_obj, value)

            {:error, :notfound} ->
              :riakc_obj.new(unquote(bucket_name), key, value)

            {:error, riakc_pb_socket_get_error} ->
              object_locator =
                "#{unquote(bucket_name)}:#{key}"
              error_message =
                inspect riakc_pb_socket_get_error
              raise "Failed to find #{object_locator} in Riak, :riakc_pb_socket.get/3 responded: #{error_message}"
          end
          |> (
               fn new_riakc_obj ->
                 :riakc_pb_socket.put(connection_pid, new_riakc_obj)
               end
               ).()
          |> case do
               :ok ->
                 :unit

               {:error, riakc_pb_socket_put_error} ->
                 object_locator =
                   "#{unquote(bucket_name)}:#{key}"
                 error_message =
                   inspect riakc_pb_socket_put_error
                 raise "Failed to find #{object_locator} in Riak, :riakc_pb_socket.get/3 responded: #{error_message}"
             end
        end
      end
    end
  end
end

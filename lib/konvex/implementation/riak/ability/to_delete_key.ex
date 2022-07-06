defmodule Konvex.Implementation.Riak.Ability.ToDeleteKey do
  @doc """
  Value type specification is compulsory
  due to Riak library has different delete functions for different data types
  (it has Riak.delete/3 for regular KV-objects and Riak.delete/4 for CRDTs)
  https://docs.riak.com/riak/kv/2.2.3/developing/key-value-modeling/index.html#bucket-types-as-additional-namespaces
  """
  defmacro __using__(
             [
               bucket_name: quoted_bucket_name,
               connection: quoted_riak_connection,
               crdt_name: quoted_crdt_name,
               value_type: :crdt
             ]
           ) do
    quote do
      import Konvex.Implementation.Riak.Connection.Usage, only: [using: 2]

      @behaviour Konvex.Ability.ToDeleteKey

      @impl Konvex.Ability.ToDeleteKey
      @spec delete_key(key :: String.t) :: :unit
      def delete_key(key) when is_binary(key) do
        using unquote(quoted_riak_connection), fn connection_pid ->
          case :riakc_pb_socket.delete(connection_pid, {unquote(quoted_crdt_name), unquote(quoted_bucket_name)}, key) do
            :ok ->
              :unit

            {:error, riakc_pb_socket_delete_error} ->
              object_locator =
                "#{unquote(quoted_bucket_name)}:#{key}"
              error_message =
                inspect riakc_pb_socket_delete_error
              raise "Failed to delete #{object_locator} from Riak, :riakc_pb_socket.delete/3 responded: #{error_message}"
          end
        end
      end
    end
  end

  defmacro __using__(
             [
               bucket_name: quoted_bucket_name,
               connection: quoted_riak_connection,
               value_type: :text
             ]
           ) do
    quote do
      import Konvex.Implementation.Riak.Connection.Usage, only: [using: 2]

      @behaviour Konvex.Ability.ToDeleteKey

      @impl Konvex.Ability.ToDeleteKey
      @spec delete_key(key :: String.t) :: :unit
      def delete_key(key) when is_binary(key) do
        using unquote(quoted_riak_connection), fn connection_pid ->
          case :riakc_pb_socket.delete(connection_pid, unquote(quoted_bucket_name), key) do
            :ok ->
              :unit

            {:error, riakc_pb_socket_delete_error} ->
              object_locator =
                "#{unquote(quoted_bucket_name)}:#{key}"
              error_message =
                inspect riakc_pb_socket_delete_error
              raise "Failed to delete #{object_locator} from Riak, :riakc_pb_socket.delete/3 responded: #{error_message}"
          end
        end
      end
    end
  end
end

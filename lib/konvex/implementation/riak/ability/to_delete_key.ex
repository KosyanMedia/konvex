defmodule Konvex.Implementation.Riak.Ability.ToDeleteKey do
  @doc """
  Value type specification is compulsory
  due to Riak library has different delete functions for different data types
  (it has Riak.delete/3 for regular KV-objects and Riak.delete/4 for CRDTs)
  https://docs.riak.com/riak/kv/2.2.3/developing/key-value-modeling/index.html#bucket-types-as-additional-namespaces
  """
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection_provider: quoted_riak_connection_provider,
               crdt_name: <<_, _ :: binary>> = crdt_name,
               value_type: :crdt
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      @behaviour Konvex.Ability.ToDeleteKey

      @impl Konvex.Ability.ToDeleteKey
      @spec delete_key(key :: String.t) :: :unit
      def delete_key(key) when is_binary(key) do
        connection_pid =
          Connection.Provider.get_connection_pid(unquote(quoted_riak_connection_provider))
        case Riak.delete(
               connection_pid,
               unquote(crdt_name),
               unquote(bucket_name),
               key
             ) do
          :ok ->
            :unit

          {:error, some_reason_from_riakc_pb_socket_delete} ->
            raise "Failed to delete #{unquote(bucket_name)}<#{unquote(crdt_name)}>:#{key} from Riak, :riakc_pb_socket.delete responded: #{inspect some_reason_from_riakc_pb_socket_delete}"
        end
      end
    end
  end

  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection_provider: quoted_riak_connection_provider,
               value_type: :text
             ]
           ) do
    quote do
      alias Konvex.Implementation.Riak.Connection

      @behaviour Konvex.Ability.ToDeleteKey

      @impl Konvex.Ability.ToDeleteKey
      @spec delete_key(key :: String.t) :: :unit
      def delete_key(key) when is_binary(key) do
        connection_pid =
          Connection.Provider.get_connection_pid(unquote(quoted_riak_connection_provider))
        case Riak.delete(
               connection_pid,
               unquote(bucket_name),
               key
             ) do
          :ok ->
            :unit

          {:error, some_reason_from_riakc_pb_socket_delete} ->
            raise "Failed to delete #{unquote(bucket_name)}:#{key} from Riak, :riakc_pb_socket.delete responded: #{inspect some_reason_from_riakc_pb_socket_delete}"
        end
      end
    end
  end
end

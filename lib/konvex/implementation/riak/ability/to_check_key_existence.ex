defmodule Konvex.Implementation.Riak.Ability.ToCheckKeyExistence do
  @doc """
  Value type specification is compulsory
  due to Riak library has different find functions for different data types
  (it has Riak.find/3 for regular KV-objects and Riak.find/4 for CRDTs)
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

      @behaviour Konvex.Ability.ToCheckKeyExistence

      @impl Konvex.Ability.ToCheckKeyExistence
      @spec has?(key :: String.t) :: boolean
      def has?(<<_, _ :: binary>> = key) when is_binary(key) do
        connection_pid =
          Connection.Provider.get_connection_pid(unquote(quoted_riak_connection_provider))
        case Riak.find(
               connection_pid,
               unquote(crdt_name),
               unquote(bucket_name),
               key
             ) do
          nil ->
            false

          {:error, some_reason_from_riakc_pb_socket_fetch_type} ->
            raise "Failed to find #{unquote(bucket_name)}<#{unquote(crdt_name)}>:#{key} in Riak, :riakc_pb_socket.fetch_type responded: #{inspect some_reason_from_riakc_pb_socket_fetch_type}"

          _key_value_object ->
            true
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

      @behaviour Konvex.Ability.ToCheckKeyExistence

      @impl Konvex.Ability.ToCheckKeyExistence
      @spec has?(key :: String.t) :: boolean
      def has?(<<_, _ :: binary>> = key) when is_binary(key) do
        connection_pid =
          Connection.Provider.get_connection_pid(unquote(quoted_riak_connection_provider))
        case Riak.find(
               connection_pid,
               unquote(bucket_name),
               key
             ) do
          nil ->
            false

          {:error, some_reason_from_riakc_pb_socket_get} ->
            raise "Failed to find #{unquote(bucket_name)}:#{key} in Riak, :riakc_pb_socket.get responded: #{inspect some_reason_from_riakc_pb_socket_get}"

          _key_value_object ->
            true
        end
      end
    end
  end
end

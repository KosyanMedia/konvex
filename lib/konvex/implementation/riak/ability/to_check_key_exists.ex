defmodule Konvex.Implementation.Riak.Ability.ToCheckKeyExists do
  @doc """
  Value type specification is compulsory
  due to Riak library has different find functions for different data types
  (it has Riak.find/3 for regular KV-objects and Riak.find/4 for CRDTs)
  https://docs.riak.com/riak/kv/2.2.3/developing/key-value-modeling/index.html#bucket-types-as-additional-namespaces
  """
  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection: quoted_riak_connection,
               crdt_name: <<_, _ :: binary>> = crdt_name,
               value_type: :crdt
             ]
           ) do
    quote do
      import Konvex.Implementation.Riak.Connection.Usage, only: [using: 2]

      @behaviour Konvex.Ability.ToCheckKeyExists

      @impl Konvex.Ability.ToCheckKeyExists
      @spec key_exists?(key :: String.t) :: boolean
      def key_exists?("" = _empty_key) do
        # :riakc does not support empty keys, so no way to store such
        false
      end

      def key_exists?(<<_, _ :: binary>> = key) do
        using unquote(quoted_riak_connection), fn connection_pid ->
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
  end

  defmacro __using__(
             [
               bucket_name: <<_, _ :: binary>> = bucket_name,
               connection: quoted_riak_connection,
               value_type: :text
             ]
           ) do
    quote do
      import Konvex.Implementation.Riak.Connection.Usage, only: [using: 2]

      @behaviour Konvex.Ability.ToCheckKeyExists

      @impl Konvex.Ability.ToCheckKeyExists
      @spec key_exists?(key :: String.t) :: boolean
      def key_exists?("" = _empty_key) do
        # :riakc does not support empty keys, so no way to store such
        false
      end

      def key_exists?(key) when is_binary(key) do
        using unquote(quoted_riak_connection), fn connection_pid ->
          case Riak.find(
                 connection_pid,
                 unquote(bucket_name),
                 key
               ) do
            nil ->
              false

            {:error, some_reason_from_riakc_pb_socket_get} ->
              raise "Failed to find #{unquote(bucket_name)}:#{key} in Riak, :riakc_pb_socket.get responded: #{inspect some_reason_from_riakc_pb_socket_get}"

            _key_value_object_or_siblings_list ->
              true
          end
        end
      end
    end
  end
end

defmodule Konvex.Implementation.Riak.Ability.ToCheckKeyExists do
  @doc """
  Value type specification is compulsory as it's an additional KV-namespace
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
          case :riakc_pb_socket.fetch_type(
                 connection_pid,
                 {unquote(crdt_name), unquote(bucket_name)},
                 key
               ) do
            {:ok, _crdt_object} ->
              true

            {:error, {:notfound, :set}} ->
              false

            {:error, riakc_pb_socket_fetch_type_error} ->
              object_locator =
                "#{unquote(bucket_name)}<#{unquote(crdt_name)}>:#{key}"
              error_message =
                inspect riakc_pb_socket_fetch_type_error
              raise "Failed to find #{object_locator} in Riak, :riakc_pb_socket.fetch_type/3 responded: #{error_message}"
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
          case :riakc_pb_socket.get(connection_pid, unquote(bucket_name), key) do
            {
              :ok,
              {
                :riakc_obj,
                unquote(bucket_name),
                object_key,
                causal_context,
                # Can be the only one or a list of conflicting sibling values
                object_values,
                # Uncommitted new value metadata as Erlang dict
                :undefined,
                # Uncommitted new value
                :undefined
              }
            } when object_key === key and is_binary(causal_context) and is_list(object_values) ->
              true

            {:error, :notfound} ->
              false

            {:error, riakc_pb_socket_get_error} ->
              object_locator =
                "#{unquote(bucket_name)}:#{key}"
              error_message =
                inspect riakc_pb_socket_get_error
              raise "Failed to find #{object_locator} in Riak, :riakc_pb_socket.get/3 responded: #{error_message}"
          end
        end
      end
    end
  end
end

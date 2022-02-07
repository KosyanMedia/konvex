defmodule Konvex.Implementation.Riak.Connection.Usage do
  alias Konvex.Implementation.Riak.Connection

  @spec using(
          connection_resource :: Connection.t,
          connection_resource_usage :: ((opened_connection_pid :: pid) -> (usage_result :: any))
        ) :: (usage_result :: any)
  def using(connection_resource, connection_resource_usage) do
    with opened_connection_pid when is_pid(opened_connection_pid)
         <- Connection.open(connection_resource) do
      try do
        connection_resource_usage.(opened_connection_pid)
      after
        Connection.close(connection_resource, opened_connection_pid)
      end
    end
  end
end

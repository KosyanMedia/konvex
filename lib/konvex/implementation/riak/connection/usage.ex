defmodule Konvex.Implementation.Riak.Connection.Usage do
  alias Konvex.Implementation.Riak.Connection

  @spec using(
          connection_resource :: Connection.t,
          connection_resource_usage :: ((opened_connection_pid :: pid) -> (usage_result :: any))
        ) :: (usage_result :: any)
  def using(connection_resource, connection_resource_usage) do
    Resource.create(
      acquire: fn -> Connection.open(connection_resource) end,
      release: &Connection.close(connection_resource, &1)
    )
    |> Resource.use!(&connection_resource_usage.(&1))
  end
end

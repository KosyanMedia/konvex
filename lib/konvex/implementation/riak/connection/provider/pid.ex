defimpl Konvex.Implementation.Riak.Connection.Provider, for: PID do
  @moduledoc """
  So you can feel free to pass connection PID itself as a connection provider
  """

  def get_connection_pid(pid), do: pid
end

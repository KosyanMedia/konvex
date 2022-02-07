defimpl Konvex.Implementation.Riak.Connection, for: PID do
  @moduledoc """
  So you can feel free to pass connection PID itself as a connection resource for Riak-clients
  """

  def open(pid), do: pid

  def close(pid, pid), do: :unit
end

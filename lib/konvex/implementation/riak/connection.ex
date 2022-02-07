defprotocol Konvex.Implementation.Riak.Connection do
  @moduledoc """
  Abstracts Riak connection in a Resource-way (obtain-use-free).

  To interact with Riak we use :riakc_pb_socket which utilizes connection PID.
  This abstraction encompasses freedom of library client's choice
  whether to use connection pooling or not, or which pooling library to select, etc.
  """

  @spec open(connection :: t) :: pid
  def open(connection)

  @spec close(connection :: t, opened_connection_pid :: pid) :: :unit
  def close(connection, opened_connection_pid)
end

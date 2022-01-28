defprotocol Konvex.Implementation.Riak.Connection.Provider do
  @moduledoc """
  Abstracts Riak connection.

  To interact with Riak we use :riakc_pb_socket which utilizes connection PID.
  This abstraction encompasses freedom of library client's choice
  whether to use connection pooling or not, or which pooling library to select, etc.
  """

  @spec get_connection_pid(connection_provider :: t) :: pid
  def get_connection_pid(connection_provider)
end

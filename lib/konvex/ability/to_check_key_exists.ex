defmodule Konvex.Ability.ToCheckKeyExists do
  @doc """
  Checks whether key is associated with any (non-nil) value
  """
  @callback key_exists?(key :: String.t) :: boolean
end

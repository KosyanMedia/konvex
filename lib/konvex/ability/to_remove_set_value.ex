defmodule Konvex.Ability.ToRemoveSetValue do
  @doc """
  Removes value from the set associated with the key
  """
  @callback remove(key :: String.t, value :: String.t) :: :key_not_found | :unit
end

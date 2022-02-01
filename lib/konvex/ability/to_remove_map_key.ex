defmodule Konvex.Ability.ToRemoveMapKey do
  @doc """
  Removes entity with key map_key from the map associated with the key
  """
  @callback remove(key :: String.t, map_key :: String.t) :: :key_not_found | :unit
end

defmodule Konvex.Ability.ToUpdateMapValue do
  @doc """
  Updates value of the entity with key map_key from the map associated with the key
  """
  @callback update(key :: String.t, map_key :: String.t, update :: (old_value :: String.t -> new_value :: String.t))
            :: :key_not_found | :unit
end

defmodule Konvex.Ability.ToAddSetValue do
  @doc """
  Adds value to the set associated with the key
  """
  @callback add(key :: String.t, value :: String.t) :: :key_not_found | :unit
end

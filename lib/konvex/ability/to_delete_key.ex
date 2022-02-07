defmodule Konvex.Ability.ToDeleteKey do
  @callback delete_key(key :: String.t) :: :unit
end

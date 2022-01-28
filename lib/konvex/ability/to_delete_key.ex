defmodule Konvex.Ability.ToDeleteKey do
  @callback delete(key :: String.t) :: :unit
end

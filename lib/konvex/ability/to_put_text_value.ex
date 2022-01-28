defmodule Konvex.Ability.ToPutTextValue do
  @callback put(key :: String.t, value :: String.t) :: :unit
end

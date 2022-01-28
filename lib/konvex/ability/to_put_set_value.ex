defmodule Konvex.Ability.ToPutSetValue do
  @callback put(key :: String.t, value :: MapSet.t(String.t)) :: :unit
end

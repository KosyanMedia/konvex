defmodule Konvex.Ability.ToPutMapValue do
  @callback put(key :: String.t, value :: %{key :: String.t => value :: String.t}) :: :unit
end

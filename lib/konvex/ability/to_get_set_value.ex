defmodule Konvex.Ability.ToGetSetValue do
  @callback get(key :: String.t) :: :key_not_found | MapSet.t(String.t)
end

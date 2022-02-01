defmodule Konvex.Ability.ToGetMapValue do
  @callback get(key :: String.t) :: :key_not_found | %{key :: String.t => value :: String.t}
end

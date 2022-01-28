defmodule Konvex.Ability.ToGetTextValue do
  @callback get(key :: String.t) :: :key_not_found | String.t
end

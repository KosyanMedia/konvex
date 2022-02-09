defmodule Konvex.Ability.ToGetAnyValue do
  @callback get_any_value(key :: String.t) :: :key_not_found | any
end

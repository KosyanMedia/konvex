defmodule Konvex.Ability.ToPutAnyValue do
  @callback put_any_value(key :: String.t, value :: any) :: :unit
end

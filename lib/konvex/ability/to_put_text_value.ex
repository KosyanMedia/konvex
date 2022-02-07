defmodule Konvex.Ability.ToPutTextValue do
  @callback put_text_value(key :: String.t, value :: String.t) :: :unit
end

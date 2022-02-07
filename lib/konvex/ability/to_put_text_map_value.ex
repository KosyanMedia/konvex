defmodule Konvex.Ability.ToPutTextMapValue do
  @callback put_text_map_value(key :: String.t, value :: %{key :: String.t => value :: String.t}) :: :unit
end

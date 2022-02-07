defmodule Konvex.Ability.ToPutTextSetValue do
  @callback put_text_set_value(key :: String.t, value :: MapSet.t(String.t)) :: :unit
end

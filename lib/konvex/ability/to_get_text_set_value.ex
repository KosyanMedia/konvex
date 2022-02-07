defmodule Konvex.Ability.ToGetTextSetValue do
  @callback get_text_set_value(key :: String.t) :: :key_not_found | MapSet.t(String.t)
end

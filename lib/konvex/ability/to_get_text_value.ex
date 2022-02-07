defmodule Konvex.Ability.ToGetTextValue do
  @callback get_text_value(key :: String.t) :: :key_not_found | String.t
end

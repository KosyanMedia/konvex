defmodule Konvex.Ability.ToGetTextMapValue do
  @callback get_text_map_value(key :: String.t) :: :key_not_found | %{key :: String.t => value :: String.t}
end

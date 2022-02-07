defmodule Konvex.Ability.ToGetAllTextKeyValues do
  @callback get_all_text_key_values() :: %{key :: String.t => value :: String.t}
end

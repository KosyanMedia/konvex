defmodule Konvex.Ability.ToGetAllTextValues do
  @callback get_all_text_values() :: MapSet.t(key :: String.t)
end

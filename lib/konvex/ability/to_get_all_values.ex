defmodule Konvex.Ability.ToGetAllValues do
  @callback get_all_values() :: MapSet.t(key :: String.t)
end

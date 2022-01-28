defmodule Konvex.Ability.ToGetAllKeys do
  @callback get_all_keys() :: MapSet.t(key :: String.t)
end

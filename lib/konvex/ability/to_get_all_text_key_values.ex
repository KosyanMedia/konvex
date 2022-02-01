defmodule Konvex.Ability.ToGetAllTextKeyValues do
  @callback get_all_key_values() :: %{key :: String.t => value :: String.t}
end

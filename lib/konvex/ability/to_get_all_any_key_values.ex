defmodule Konvex.Ability.ToGetAllAnyKeyValues do
  @callback get_all_any_key_values() :: %{key :: String.t => value :: any}
end

defmodule Konvex.Ability.ToCheckKeyExistence do
  @callback has?(key :: String.t) :: boolean
end

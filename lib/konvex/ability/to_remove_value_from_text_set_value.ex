defmodule Konvex.Ability.ToRemoveValueFromTextSetValue do
  @doc """
  Removes value from the set associated with the key
  """
  @callback remove_value_from_text_set_value(key :: String.t, value :: String.t) :: :key_not_found | :unit
end

defmodule Konvex.Ability.ToAddValueToTextSetValue do
  @doc """
  Adds value to the set associated with the key
  """
  @callback add_value_to_text_set_value(key :: String.t, value :: String.t) :: :key_not_found | :unit
end

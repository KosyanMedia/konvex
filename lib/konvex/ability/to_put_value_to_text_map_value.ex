defmodule Konvex.Ability.ToPutValueToTextMapValue do
  @doc """
  Sets map_key to store value in the map associated with the key
  """
  @callback put_value_to_text_map_value(key :: String.t, map_key :: String.t, value :: String.t)
            :: :key_not_found | :unit
end

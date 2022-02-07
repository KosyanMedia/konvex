defmodule Konvex.Implementation.Riak.ConflictResolutionStrategy.LastWriteWins do
  @behaviour Konvex.Implementation.Riak.ConflictResolutionStrategy

  @impl Konvex.Implementation.Riak.ConflictResolutionStrategy
  @spec resolve(conflicting_sibling_values :: [String.t, ...], bucket_name :: String.t, key :: String.t)
        :: (selected_one :: String.t)
  def resolve([<<_ :: binary>> = the_last_one | _] = _conflicting_sibling_values, bucket_name, key)
      when is_binary(bucket_name) and is_binary(key) do
    the_last_one
  end
end

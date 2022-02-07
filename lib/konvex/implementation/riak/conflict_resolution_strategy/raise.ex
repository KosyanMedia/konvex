defmodule Konvex.Implementation.Riak.ConflictResolutionStrategy.Raise do
  @behaviour Konvex.Implementation.Riak.ConflictResolutionStrategy

  @impl Konvex.Implementation.Riak.ConflictResolutionStrategy
  @spec resolve(conflicting_sibling_values :: [String.t, ...], bucket_name :: String.t, key :: String.t)
        :: (raised_exception :: none)
  def resolve([<<_ :: binary>> = _the_last_one | _] = conflicting_sibling_values, bucket_name, key)
      when is_binary(bucket_name) and is_binary(key) do
    raise "Found siblings in #{bucket_name}:#{key}: #{inspect conflicting_sibling_values}"
  end
end

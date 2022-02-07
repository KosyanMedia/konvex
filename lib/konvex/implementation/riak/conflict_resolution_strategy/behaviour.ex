defmodule Konvex.Implementation.Riak.ConflictResolutionStrategy do
  @moduledoc """
  Applicable only for regular KV-objects, i.e. text values
  (CRDT don't have conflicts on the client side)

  https://docs.riak.com/riak/kv/2.2.3/developing/usage/conflict-resolution/#siblings-in-action
  """

  @doc """
  Conflicting sibling values list starts from "the most recent" value followed by "more older" ones
  """
  @callback resolve(conflicting_sibling_values :: [String.t, ...], bucket_name :: String.t, key :: String.t)
            :: (selected_one :: String.t)
end

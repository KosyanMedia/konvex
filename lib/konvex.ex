defmodule Konvex do
  @moduledoc """
  Provides modular KV-storage abstractions.

  Each KV-storage shares a common set of abilities (e.g. get value by key),
  but at the same time can expose it's own unique one.
  So to address this heterogeneity and be able to compose modular clients
  the library exposes this abilities on their own.
  Each client defines it's own set of abilities it provides (and how each of them is implemented).
  As a consequence least privilege principle comes for free,
  and you can build clients along with your business-logic
  (e.g. you can build two clients backed by the same storage,
  but of different abilities: one for admin access with full set of abilities
  and another for user read-only access without functionality to modify storage data).

  The main KV-storage abstraction in konvex is Ability, that defines unit of storage functionality
  (that can be either unique for particular backend or common for several ones).
  Ability implementations for particular backends can be found in Konvex.Implementation module.

  Given with ability implementations client is just a module that defines a subset of them to use.
  Example:

      defmodule ReadOnlyClient do
        use Konvex.Implementation.YetAnotherKeyValueStorage.Ability.ToGetTextValue,
            # Each backend can define it's own specific set of metadata to implement communication with it
            connection_pool: get_connection_pool_for(:user_access)
      end

      defmodule AdminClient do
        use Konvex.Implementation.YetAnotherKeyValueStorage.Ability.ToGetTextValue,
            connection_pool: get_connection_pool_for(:admin_access)
        use Konvex.Implementation.YetAnotherKeyValueStorage.Ability.ToPutTextValue,
            connection_pool: get_connection_pool_for(:admin_access)
        use Konvex.Implementation.YetAnotherKeyValueStorage.Ability.ToDeleteKey,
            connection_pool: get_connection_pool_for(:admin_access)
      end

  Bonus level for those who's still reading (or library name motivation):
  konvex stands for convex hull of Abilities.
  The analogy here is that each client built with the library
  is at some point a convex hull of abilities it implements.
  The name also satisfies following requirements (joke in progress):
   * must include K, V letters as a KV-storage library
   * must include EX suffix as an Elixir-library

  Thank you for your attention (joke done)
  """
end

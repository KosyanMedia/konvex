# konvex

Library to abstract KV-storage in a modular way along with implementations for popular backends

## Description

Each KV-storage shares a common set of abilities (e.g. get value by key),
but at the same time can expose it's own unique one.
So to address this heterogeneity and be able to compose modular clients
the library exposes this abilities on their own.

Each client defines it's own set of abilities it provides (and how each of them is implemented).
As a consequence [least privilege principle](https://en.wikipedia.org/wiki/Principle_of_least_privilege)
comes for free, and you can build clients along with your business-logic
(e.g. you can build two clients backed by the same storage,
but of different abilities: one for admin access with full set of abilities
and another for user read-only access without functionality to modify storage data).

The main KV-storage abstraction in **konvex** is **Ability**, that defines unit of storage functionality
(that can be either unique for particular backend or common for several backends).
Ability implementations for particular backends can be found in `Konvex.Implementation` module.

## Installation

Not available in Hex yet, however the library can be installed
by adding `konvex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:konvex, github: "KosyanMedia/konvex"}
  ]
end
```

## Example

```elixir
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
```
## Bonus level (or library name motivation)

**konvex** stands for [convex hull](https://en.wikipedia.org/wiki/Convex_hull) of **Abilities**.

The analogy here is that each client built with the library
is at some point a convex hull of abilities it implements.

The name also satisfies following requirements (joke in progress):
* must include **K**, **V** letters as a KV-storage library
* must include **EX** suffix as an Elixir-library

Thank you for your attention (joke done)

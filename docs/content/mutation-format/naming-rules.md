# Naming rules

Dmut understands `.` separators in mutation names. A mutation named `parent1.parent2.child`
automatically depends on `parent1` and `parent1.parent2` if they exist. It will **not** depend on
`parent1.unrelated`. The name of a mutation file is its name, so `parent1.parent2.child.yml` defines
it; the directory it is in does not matter.

Duplicate mutation names within the same namespace and revision are an error, including across
different files — the error names both files.

Dependency cycles are detected at load time and reported with the offending path.

## A convention: name every schema object `<schema>.<object>`

The [example database](../example-database/index.md) names its schema mutation `hotel`, and then
names *every mutation that creates something inside that schema* `hotel.<name>` —
`hotel.chains`, `hotel.properties`, `hotel.rooms`, and so on for every table — instead of leaving
them as flat top-level names like `chains` or `rooms`. That's a deliberate convention, not
something dmut requires: every one of those mutations picks up a dependency on `hotel` **for
free**, with no `deps: [hotel]` anywhere, because dotted names automatically depend on whichever
of their ancestor segments happen to be declared mutations.

Two things in this schema are deliberately *not* dotted under `hotel`: `roles` and `extensions`.
A role is cluster-wide and an extension isn't scoped to any one schema either, so neither is
actually an object "in" the `hotel` schema — dotting them under it would claim a relationship that
isn't real.

## `deps` targets a mutation, not its descendants

Naming a mutation in `deps` does **not** transitively pull in mutations dotted underneath it.
`hotel.rooms` uses the `hotel.room_status` enum type, declared as `hotel.types.room_status`.
Depending on `hotel` alone would only guarantee the schema itself exists first, not that the enum
type does:

```yaml
--8<-- "examples/hotel/hotel.rooms.yml:1:6"
```

The same pattern shows up for every other composite/enum type used by a table: `hotel.guests`
depends on `hotel.types.address`, `hotel.payment_methods` on `hotel.types.card_summary`,
`hotel.bookings` on `hotel.types.booking_status`, `hotel.payments` on `hotel.types.payment_status`.
Each names the specific type it actually uses, not just `hotel`. It works the same way one level up:
`hotel.properties` depends on `hotel.chains` explicitly, `hotel.room_types` on `hotel.properties`
explicitly, and so on down the whole dependency graph in [the schema
reference](../example-database/reference.md) — dotting everything under `hotel` only ever buys the
one shared ancestor, never a sibling.

## Child mutations

A child is just a mutation whose name extends its parent's: `hotel.properties.description_search`
is written in its own file and depends on `hotel.properties` through its name. Reach for one when
the child is genuinely subordinate to one specific parent and likely to be added, changed or dropped
on its own later, without disturbing the parent's definition. Adding a column to a table can also be
just one more statement at the end of the table's `sql`; a child is for what has a life of its own.

```yaml
--8<-- "examples/hotel/hotel.properties.description_search.yml"
```

`hotel.types.address` automatically depends on `hotel` even though no mutation named `hotel.types`
exists anywhere — the parent-lookup that dotted names trigger simply skips any segment that isn't
itself a declared mutation, so only `hotel` needs to be real for every `hotel.types.*` mutation to
depend on it.

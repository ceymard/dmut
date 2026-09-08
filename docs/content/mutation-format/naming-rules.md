# Naming rules and children

Dmut understands `.` separators in mutation names. A mutation named `parent1.parent2.child`
automatically depends on `parent1` and `parent1.parent2` if they exist, for both `sql` and `meta`.
It will **not** depend on `parent1.unrelated`.

Mutations declared under `children:` are named `<parent>.<child>` and therefore pick up that
dependency automatically.

Duplicate mutation names within the same namespace and revision are an error, including across
different files — the error names both files.

Dependency cycles are detected at load time and reported with the offending path.

## A convention: name every schema object `<schema>.<object>`

The [example database](../example-database/index.md) names its schema mutation `hotel`, and then
names *every mutation that creates something inside that schema* `hotel.<name>` —
`hotel.chains`, `hotel.properties`, `hotel.rooms`, and so on for every table — instead of leaving
them as flat top-level names like `chains` or `rooms`. That's a deliberate convention, not
something dmut requires: every one of those mutations picks up a dependency on `hotel` **for
free**, with no `needs: [hotel]` anywhere, because dotted names automatically depend on whichever
of their ancestor segments happen to be declared mutations.

Two things in this schema are deliberately *not* dotted under `hotel`: `roles` and `extensions`.
A role is cluster-wide and an extension isn't scoped to any one schema either, so neither is
actually an object "in" the `hotel` schema — dotting them under it would claim a relationship that
isn't real.

## `needs` targets a mutation, not its descendants

One subtlety worth calling out explicitly: naming a mutation in `needs` does **not** transitively
pull in mutations dotted underneath it. `hotel.rooms` uses the `hotel.room_status` enum type,
declared as `hotel.types.room_status` in `hotel.yml`. Depending on `hotel` alone would only
guarantee the schema itself exists first, not that the enum type does:

```yaml
--8<-- "examples/hotel/rooms.yml:1:5"
```

The same pattern shows up for every other composite/enum type used by a table further down the
dependency graph — `hotel.guests` needs `hotel.types.address`, `hotel.payment_methods` needs
`hotel.types.card_summary`, `hotel.bookings` needs `hotel.types.booking_status`, `hotel.payments`
needs `hotel.types.payment_status`. Each names the specific type it actually uses, not just
`hotel`. It works the same way one level up, too: `hotel.properties` needs `hotel.chains`
explicitly, `hotel.room_types` needs `hotel.properties` explicitly, and so on down the whole
dependency graph in [the schema reference](../example-database/reference.md) — dotting everything
under `hotel` only ever buys the one shared ancestor, never a sibling.

## When to reach for `children` instead

Dotted names and `children:` both produce the same `<parent>.<child>` mutation name and the same
automatic dependency — `children:` is just a way to write the child's block nested inside the
parent's, in the same file, instead of as its own top-level key. Reach for it when the child is
genuinely subordinate to one specific parent and likely to be added, changed or dropped on its own
later, without disturbing the parent's own definition — not for a shared dependency between things
that aren't otherwise related to each other (that's what dotting everything under `hotel` is for,
above).

The [example database](../example-database/index.md)'s `hotel.properties` mutation uses `children`
for exactly that case — a generated column and its index, kept separate from the table's own
definition:

```yaml
--8<-- "examples/hotel/properties.yml"
```

`examples/hotel/hotel.yml` shows the other shape — the schema itself and every composite/enum type
it uses, as flat, top-level, dotted names in one file, with no intermediate grouping mutation:

```yaml
--8<-- "examples/hotel/hotel.yml"
```

`hotel.types.address` automatically depends on `hotel` even though no mutation named `hotel.types`
exists anywhere — the parent-lookup that dotted names trigger simply skips any segment that isn't
itself a declared mutation, so only `hotel` needs to be real for every `hotel.types.*` mutation to
depend on it.

# Walkthrough

Almost every mutation in `examples/hotel/` is named `hotel.<object>` — see [Naming
rules](../mutation-format/naming-rules.md#a-convention-name-every-schema-object-schemaobject) for
why. Each lives in a file named after it, so `hotel.rooms.yml` holds `hotel.rooms`.

## Starting from nothing: `hotel` and `extensions`

```yaml
--8<-- "examples/hotel/hotel.yml"
```

The schema is named `hotel`, not `schema`, so that everything else can be dotted `hotel.<object>`
and depend on it for free. Every composite and enum type has a mutation of its own, named
`hotel.types.address`, `hotel.types.card_summary`, and so on. There's no intermediate `hotel.types`
mutation anywhere; dotted-name auto-dependency only needs the segments that actually exist as
mutations, so `hotel.types.address` depends on `hotel` directly.

```yaml
--8<-- "examples/hotel/extensions.yml"
```

`extensions` (and, in its own file, `roles`) are the two mutations in this schema *not* dotted
under `hotel` — an extension isn't scoped to one schema and a role is cluster-wide, so neither is
actually an object "in" the `hotel` schema. `btree_gist` itself is required by `hotel.bookings`'
exclusion constraint but isn't conceptually part of any one table — it's infrastructure the schema
as a whole depends on.

## A table and the things that hang off it

```yaml
--8<-- "examples/hotel/hotel.rooms.yml"
```

A table is a structural mutation. It lists in `deps` what it references: the properties and room
types its foreign keys point to, and the enum type of its `status` column. Depending on `hotel`
alone would only guarantee that the schema exists, so each type is named explicitly.

Everything lightweight that goes with a table is a mutation of its own, named as a child of the
table:

```yaml
--8<-- "examples/hotel/hotel.rooms.functions.yml"
```

`hotel.rooms.functions` is a default mutation: it holds functions and comments, which can be taken
down and brought back up without losing a row. It depends on `hotel.rooms` through its dotted name,
and on `hotel.room_types` because `room_effective_rate` reads it. `reapply_on: [dep_reapplied,
dep_appended]` makes it follow its dependencies: when a column is appended to a table the function
reads, the function is reapplied.

## A generated column: `hotel.properties.description_search`

```yaml
--8<-- "examples/hotel/hotel.properties.description_search.yml"
```

A stored generated column and its GIN index are a mutation of their own, because they are the kind of
thing you might want to alter or drop independently of the table later. `search_properties`, the
function in `hotel.properties.functions`, depends on the column, so it lists
`hotel.properties.description_search` in its `deps`.

## Grants that follow the schema

```yaml
--8<-- "examples/hotel/hotel.grants.yml"
```

A plain `grant select on all tables in schema hotel` only covers what exists *at the moment it
runs*. A table added later by a new mutation would never be granted. So `hotel.grants` depends on
`hotel.**` with `reapply_on: [dep_reapplied, dep_appended]`: when anything in the namespace is
reapplied, or gets statements appended — a new table is a new mutation, which makes the pattern
grow too — the grants run again. It is a default mutation, so reapplying it touches no data.

## Computed columns: `hotel.guests.functions`

```yaml
--8<-- "examples/hotel/hotel.guests.functions.yml"
```

`guest_full_name` is a pure computed column, and `split_name` is its inverse — built with
Postgres's `OUT`-parameter syntax to return two columns from one call. Both auto-down as plain
`CREATE FUNCTION` strings; see [Automatic down statements](../mutation-format/auto-down.md) for how
dmut builds the right `DROP FUNCTION` signature around `OUT` parameters.

## The rest

`hotel.room_types`, `hotel.amenities`, `hotel.property_amenities`, `hotel.payment_methods`,
`hotel.bookings`, `hotel.booking_guests`, `hotel.payments`, `hotel.reviews`, `hotel.rate_plans`,
`hotel.staff` and `roles` follow the same shape: a structural mutation for the table (or, for
`roles`, the role itself), a `.functions` child where there are functions. Browse them directly in
[`examples/hotel/`](https://github.com/ceymard/dmut/tree/main/examples/hotel) — every one of them is
real, tested yaml, not an excerpt trimmed for the page.

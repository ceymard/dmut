# Walkthrough

Almost every mutation in `examples/hotel/` is named `hotel.<object>` — see [Naming rules and
children](../mutation-format/naming-rules.md#a-convention-name-every-schema-object-schemaobject)
for why. Most files still hold one mutation each, named after the mutation's last dotted
component (`chains.yml` holds `hotel.chains`, `rooms.yml` holds `hotel.rooms`) — a deliberate,
readable middle ground rather than what `dmut explode` would literally produce for a dotted name
(it would nest `hotel.chains` under a `hotel/` directory). `hotel.yml` is the one file that breaks
the one-mutation-per-file pattern entirely.

## Starting from nothing: `hotel.yml` and `extensions`

```yaml
--8<-- "examples/hotel/hotel.yml"
```

This is six mutations in one file, not one — the schema itself (named `hotel`, not `schema`, so
that everything else can be dotted `hotel.<object>`) plus every composite/enum type this schema
uses, named `hotel.types.address`, `hotel.types.card_summary`, and so on. There's no intermediate
`hotel.types` mutation anywhere; dotted-name auto-dependency only needs the segments that actually
exist as mutations, so `hotel.types.address` depends on `hotel` directly.

`hotel`'s own `meta` is where the blanket grant to an unauthenticated role lives — right next to the
schema it governs, which is exactly what `meta` is for: the "how it's accessed" half of an object,
kept with that object's own mutation rather than off in a mutation of its own. It's covered in
detail in [sql vs meta](../mutation-format/sql-vs-meta.md#meta-doesnt-always-rerun-when-youd-expect):
each `grant ... on all X in schema hotel` is paired with an `alter default privileges` statement so
that a table, sequence or function added by some future mutation is covered automatically, without
anyone having to remember to touch this mutation again.

```yaml
--8<-- "examples/hotel/extensions.yml"
```

`extensions` (and, in its own file, `roles`) are the two mutations in this schema *not* dotted
under `hotel` — an extension isn't scoped to one schema and a role is cluster-wide, so neither is
actually an object "in" the `hotel` schema. `btree_gist` itself is needed by `hotel.bookings`'
exclusion constraint but isn't conceptually part of any one table — it's infrastructure the schema
as a whole depends on.

## A table with a `children` generated column: `hotel.properties`

```yaml
--8<-- "examples/hotel/properties.yml"
```

`description_search` is a stored generated column plus its GIN index, kept as a `children` entry
rather than folded into `hotel.properties`'s own `sql`. That's not a revisions concern here
(there's only one revision in this example) — it's kept separate because it's the kind of thing
you might want to alter or drop independently of the table itself later, and `search_properties`,
the `meta` function right below it, depends on the column existing. Since all `sql` (children
included) runs before any `meta`, that dependency is automatic and needs no `meta_needs`.

## Computed columns: `hotel.guests`

```yaml
--8<-- "examples/hotel/guests.yml"
```

`guest_full_name` is a pure computed column, and `split_name` is its inverse — built with
Postgres's `OUT`-parameter syntax to return two columns from one call. Both auto-down as plain
`CREATE FUNCTION` strings; see [Automatic down statements](../mutation-format/auto-down.md) for how
dmut builds the right `DROP FUNCTION` signature around `OUT` parameters.

## The rest

`hotel.room_types`, `hotel.rooms`, `hotel.amenities`, `hotel.property_amenities`,
`hotel.payment_methods`, `hotel.bookings`, `hotel.booking_guests`, `hotel.payments`,
`hotel.reviews`, `hotel.rate_plans`, `hotel.staff` and `roles` follow the same shape: a `sql` block
for the table (or, for `roles`, the role itself), a `meta` block for comments, computed-column
functions, and anything else lightweight — each one dotted under `hotel` except `roles` itself, for
the reason above. Browse them directly in [`examples/hotel/`](
https://github.com/ceymard/dmut/tree/main/examples/hotel) — every one of them is real, tested
yaml, not an excerpt trimmed for the page.

---
icon: material/office-building
---

# Example database

Every mutation-format example in this documentation is drawn from
[`examples/hotel`](https://github.com/ceymard/dmut/tree/main/examples/hotel): a small
hotel-booking schema — chains, properties, rooms, guests, bookings, payments — deep enough in
relationships and varied enough in Postgres types (composite types, enums, ranges with exclusion
constraints, generated columns, full-text search, functions in every argument-mode Postgres has)
to be a realistic tour of everything dmut supports, rather than a couple of flat tables.

The schema itself isn't invented for this documentation — it's the [rel](
https://github.com/rel-server/rel) project's own hotel-booking dev fixture (`test/hotel/schema.sql`
there), translated statement-for-statement into dmut mutations. Every table, function, comment and
grant in `examples/hotel/` corresponds to something in that original `schema.sql`.

## In this section

- **[Schema reference](reference.md)** — the full list of mutations, in dependency order, and what
  each one covers.
- **[Walkthrough](walkthrough.md)** — how the schema is actually laid out as mutations: dotting
  every mutation under `hotel.` to get the schema dependency for free, `needs` for the dependencies
  that aren't free, `children` for generated columns, and a real case where ordering among `meta`
  blocks looks like it matters and turns out not to.

## It's a real, running database

Every file in `examples/hotel/` is verified by `dmut test` — replaying the whole namespace's worth
of mutations down-then-up, independently, against a throwaway Postgres container — and by a real
`dmut apply` against a running Postgres, with the resulting schema inspected via `psql`. You can do
the same:

```sh
dmut test -v ./examples/hotel                                        # throwaway Postgres, no commit
dmut apply postgres://user:pass@localhost/mydb ./examples/hotel       # the real thing
dmut down postgres://user:pass@localhost/mydb hotel                   # and back out again
```

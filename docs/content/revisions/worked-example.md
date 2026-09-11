# Worked example

This walks through [`examples/revisions`](https://github.com/ceymard/dmut/tree/main/examples/revisions), a small, self-contained schema
(one table, no foreign keys) that tells the same story as the [example database](../example-database/index.md)'s `properties.description_search` child: full-text search over a
`description` column, added after the fact via a stored generated column — except here it's told
across three revisions of the same table, the way it would actually happen against a database
that's already in production.

Every statement below has actually been run — this documentation's build pipeline verifies it with
`dmut test -a examples/revisions` (replaying revision 1 → 2 → 3 against a throwaway Postgres
container) and, separately, with three real `dmut apply` calls against a running Postgres,
inspected with `psql` afterward.

## Revision 1: the starting point

A plain table, seeded with two rows.

```yaml
--8<-- "examples/revisions/r1.yml"
```

## Revision 2: adding search, without touching the table

`properties` needs full-text search. The column is added as a child mutation —
`properties.description_search` — rather than by editing `properties` itself, so the existing
table isn't dropped and recreated:

```yaml
--8<-- "examples/revisions/r2.yml"
```

This revision declares `delete` and `override` (see [Squash revisions](index.md)), which makes it
*override-shaped* — mutation definitions have to live nested under `mutations:` instead of as flat
top-level keys, so a mutation can never collide with one of the reserved names (`mode`, `delete`,
`override`, `mutations`). Two things are declared here that only matter for *later*:

- `override.properties` describes what a **brand-new** database will get once revision 3 also
  exists: the column and its index folded directly into `CREATE TABLE`, as if they'd always been
  there. It has to match revision 3's actual `core` for `properties` byte-for-byte (including the
  index, not just the table) — dmut hashes the whole `core` list, so a database that already has
  revision 2's override recorded and a database that jumps straight to revision 3's `core` need to
  land on the exact same hash, or dmut treats them as different definitions and drops/recreates the
  table for no reason. `override` replaces a mutation's recorded `core` *and* `glue` *and* `needs`
  wholesale, so the seed-data insert has to be restated here too — leaving it out wouldn't fail
  loudly, it would just make the *recorded* glue empty, and a later revision that still has that
  insert in its own `glue` would see it as new and run it again.
- `delete: [properties.description_search]` retires the child mutation once revision 3 lands: it
  already ran for real via `mutations:` below, so once its column is folded into `properties`'s
  override, it no longer needs separate tracking. `delete` only forgets it was being tracked — it
  never runs a down, which is exactly what's wanted here, since the column already exists and stays.

Applying revision 2 against a database that already has revision 1 only runs the *actual* new
thing, via `mutations:` — the child mutation's `ALTER TABLE ADD COLUMN` and its index. `properties`
itself doesn't move at all, because its own `core` there is unchanged from revision 1.

## Revision 3: consolidating

```yaml
--8<-- "examples/revisions/r3.yml"
```

`properties.description_search` isn't mentioned here at all — it doesn't need to be. Revision 2's
`delete` already told dmut it drops out of tracking once revision 3 lands. `properties`'s own `core`
here is exactly what revision 2's `override` recorded, so — for a database that walked through
revision 2 first — nothing physically changes at all when revision 3 is applied: dmut compares
revision 3's `core` against what's actually recorded (the override, since that's what got saved),
finds them identical, and does nothing. The `do $$ ... $$` block is the payoff: it runs inside the
same transaction as everything else and fails the whole apply if the two rows from revision 1
didn't survive, or if full-text search over the now-consolidated column doesn't work.

Running `dmut test -v -a examples/revisions` against a fresh throwaway database shows this
directly — the log for the revision 2 → 3 transition contains no `↓`/`↑` for `properties·core` at
all, only `glue`'s new statement (the `do $$` check — the seed insert is already accounted for by
revision 2's override and doesn't run again):

```
→ applying mutations for namespace revisions revision 2
↑ revisions properties.description_search·core
→ applying mutations for namespace revisions revision 3
↑ revisions properties·glue
notice   revision check passed: 2 properties, full text search still works
```

A **fresh** database only ever sees the final, consolidated shape — `dmut test examples/revisions`
(without `-a`) applies revision 3 directly, and `properties·core` is exactly the consolidated
`CREATE TABLE` plus its index, in one step, with no intermediate `ALTER TABLE` in sight.

## Running it yourself

```sh
dmut test -a ./examples/revisions            # replay 1 → 2 → 3 against a throwaway Postgres
dmut test ./examples/revisions               # a fresh database only sees revision 3

dmut apply postgres://user:pass@localhost/mydb ./examples/revisions/r1.yml
dmut apply postgres://user:pass@localhost/mydb ./examples/revisions/r1.yml ./examples/revisions/r2.yml
dmut apply postgres://user:pass@localhost/mydb ./examples/revisions
```

Running those three `apply` commands in order against a real database and checking afterward:

```sql
select id, name, description_search is not null as fts from properties;
--  id |      name      | fts
-- ----+----------------+-----
--  .. | Seaside Inn    | t
--  .. | Mountain Lodge | t
```

Both rows, inserted back in revision 1, are still there after walking all the way through to
revision 3 — and `description_search` is populated for both, proving the generated column was
actually rebuilt (not just declared) against the pre-existing data.

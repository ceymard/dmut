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

Two things are declared here that only matter for *later*:

- `properties`'s `new_sql` describes what a **brand-new** database will get once revision 3 also
  exists: the column and its index folded directly into `CREATE TABLE`, as if they'd always been
  there. It has to match revision 3's actual `sql` for `properties` byte-for-byte (including the
  index, not just the table) — dmut hashes the whole `sql` list, so a database that's already
  received the `new_sql` value in revision 2 and a database that jumps straight to revision 3's
  `sql` need to land on the exact same hash, or dmut treats them as different definitions and
  drops/recreates the table for no reason.
- `properties.description_search`'s `new_sql: []` says that by revision 3, this child mutation
  will have nothing left to contribute — it's being retired.

Applying revision 2 against a database that already has revision 1 only runs the *actual* new
thing: the child mutation's `ALTER TABLE ADD COLUMN` and its index. `properties` itself doesn't
move at all, because its own `sql` (not `new_sql`) is unchanged from revision 1.

## Revision 3: consolidating

```yaml
--8<-- "examples/revisions/r3.yml"
```

`properties.description_search` isn't mentioned here at all — it doesn't need to be. Its `new_sql`
in revision 2 already told dmut what its recorded state becomes once revision 3 lands: empty, with
nothing left to down. `properties`'s own `sql` here is exactly what revision 2 promised via
`new_sql`, so — for a database that walked through revision 2 first — nothing physically changes at
all when revision 3 is applied. The `do $$ ... $$` block is the payoff: it runs inside the same
transaction as everything else and fails the whole apply if the two rows from revision 1 didn't
survive, or if full-text search over the now-consolidated column doesn't work.

Running `dmut test -v -a examples/revisions` against a fresh throwaway database shows this
directly — the log for the revision 2 → 3 transition contains no `↓`/`↑` for `properties·sql` at
all, only the `meta` (the reinserted data statement, and the `do $$` check, both restated as
required):

```
→ applying mutations for namespace revisions revision 2
↑ revisions properties.description_search·sql
using new_* mutations from previous revision
→ applying mutations for namespace revisions revision 3
↓ revisions properties·meta
↑ revisions properties·meta
notice   revision check passed: 2 properties, full text search still works
```

A **fresh** database only ever sees the final, consolidated shape — `dmut test examples/revisions`
(without `-a`) applies revision 3 directly, and `properties·sql` is exactly the consolidated
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

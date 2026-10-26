# Worked example

This walks through [`examples/revisions`](https://github.com/ceymard/dmut/tree/main/examples/revisions), a small, self-contained schema
(one table, no foreign keys) that tells the same story as the [example database](../example-database/index.md)'s `hotel.properties.description_search` mutation: full-text search over a
`description` column, added after the fact via a stored generated column — except here it's told
across three revisions of the same table, the way it would actually happen against a database
that's already in production.

Every statement below has actually been run — this documentation's build pipeline verifies it with
`dmut test -a examples/revisions` (replaying revision 1 → 2 → 3 against a throwaway Postgres
container).

## Revision 1: the starting point

A plain table, seeded with two rows. The seed is written so that running it twice does nothing,
which dmut checks.

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

This revision declares `delete` and `override` (see [Squash revisions](index.md)). Two things are
declared here that only matter for *later*:

- `override.properties` describes what a **brand-new** database will get once revision 3 also
  exists: the column and its index folded directly into `CREATE TABLE`, as if they'd always been
  there. It has to match revision 3's actual `properties` exactly (including the index and the seed,
  not just the table) — a database that already has revision 2's override recorded and a database
  that jumps straight to revision 3 need to hold the same definition, or dmut treats them as
  different and drops and recreates the table for no reason. `override` replaces a mutation's
  recorded statements and keys wholesale, so the seed has to be restated here too — leaving it out
  wouldn't fail loudly, it would just make the *recorded* mutation shorter, and revision 3, which
  still has the seed, would see it as new and run it again.
- `delete: [properties.description_search]` retires the child mutation once revision 3 lands: it
  already ran for real via `mutations:` below, so once its column is folded into `properties`'s
  override, it no longer needs separate tracking. `delete` only forgets it was being tracked — it
  never runs a down, which is exactly what's wanted here, since the column already exists and stays.

Applying revision 2 against a database that already has revision 1 only runs the *actual* new
thing, via `mutations:` — the child mutation's `ALTER TABLE ADD COLUMN` and its index. `properties`
itself doesn't move at all, because its own statements there are unchanged from revision 1.

## Revision 3: consolidating

```yaml
--8<-- "examples/revisions/r3.yml"
```

`properties.description_search` isn't mentioned here at all — it doesn't need to be. Revision 2's
`delete` already told dmut it drops out of tracking once revision 3 lands. `properties` here is
exactly what revision 2's `override` recorded, so — for a database that walked through revision 2
first — nothing physically changes at all for it: dmut compares revision 3's statements against what
is actually recorded (the override, since that's what got saved), finds them identical, and does
nothing to the table.

`properties.revision_check` is the payoff. It is a default mutation of two `ensure:` checks, which
run inside the same transaction as everything else and fail the whole apply if the two rows from
revision 1 didn't survive, or if full-text search over the now-consolidated column doesn't work. It
depends on `properties` through its name, and it is new at revision 3, so only that mutation runs
when a database at revision 2 is carried to revision 3.

A **fresh** database only ever sees the final, consolidated shape — `dmut test examples/revisions`
(without `-a`) applies revision 3 directly, and `properties` is exactly the consolidated
`CREATE TABLE` plus its index and seed, in one step, with no intermediate `ALTER TABLE` in sight.

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

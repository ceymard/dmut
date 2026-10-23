# core vs glue

`core` blocks contain the physical description of your data — "*what*" will be accessed and
modified — whereas `glue` blocks describe "*how*" (and *by whom*) it is accessed. Changes in the
`core` block can be heavy and lead to loss of data or long processing times. `glue` changes are
mostly code and will thus be pretty fast.

The two are hashed separately, so editing a function or a grant never causes a table to be
dropped.

Glue could be separate mutations (as in earlier dmut versions), but that approach gets messy:
dependency graphs and naming conventions are hard to agree on. Keeping glue next to the objects it
manages is simpler, and all `core` runs before any glue, so objects and roles are guaranteed to
exist when glue references them. The split also encourages thinking in terms of heavy (core) vs
light (glue) changes.

## What goes in `core`

Statements whose down would lose data or take a long time:

- `CREATE TABLE ...`
- `CREATE MATERIALIZED VIEW ...` (it holds data, so it is never glue)
- `CREATE INDEX ...` (no data is lost, but indexes can be slow to create)
- `CREATE TYPE ...`
- `CREATE SCHEMA ...`
- `CREATE EXTENSION ...`
- `CREATE ROLE ...`
- `ALTER TABLE <table> ADD COLUMN ...`
- `ALTER TABLE <table> ADD CONSTRAINT ...`

## What goes in `glue`

Lightweight statements that describe behaviour and can be de-applied and re-applied often:

- `CREATE FUNCTION ...` / `CREATE PROCEDURE ...`
- `CREATE VIEW ...`
- `CREATE TRIGGER ...`
- `CREATE POLICY ...`
- `ALTER TABLE <table> ENABLE ROW LEVEL SECURITY`
- `ALTER TABLE <table> ALTER COLUMN <name> SET DEFAULT ...`
- `GRANT ...`
- `ALTER DEFAULT PRIVILEGES ...`

## Glue doesn't always rerun when you'd expect

Dmut only redoes *all* `glue` in a namespace when some `core` had to be downed (see [How a run
works](../getting-started/how-a-run-works.md)). A purely additive change — a brand-new table added
in some future mutation, with nothing removed or altered — never downs anything, so it does **not**
trigger a full glue rebuild. Any `glue` that was written assuming it would "catch" objects created
later can quietly stop being true the moment a schema grows in this specific, harmless-looking way.

The [example database](../example-database/index.md)'s `hotel` mutation runs into this directly. Its
`glue` grants blanket access to an unauthenticated role — the schema itself, right next to the
grants that govern it, is exactly what `glue` is for:

```yaml
--8<-- "examples/hotel/hotel.yml"
```

A plain `grant select, insert, update, delete on all tables in schema hotel` (and the equivalent for
sequences and functions) only covers what exists *at the moment it runs*. Since tables and sequences
are `core` and this grant is `glue`, every table that exists when `hotel` was first applied is always
already covered — `core` always runs before `glue`. But a table added by some *future* mutation,
added without downing anything, won't force `hotel`'s `glue` to rerun, and the grant would silently
never reach it.

`alter default privileges` closes that gap for all three object kinds: it doesn't act on what
already exists, it tells Postgres to grant automatically to whatever the same role creates
*afterward*, with no dmut involvement needed at all. Functions have a second, independent reason to
want this: they're created via `glue` themselves (`CREATE FUNCTION` is a `glue` statement, spread
across six different table mutations here), so ordering *among* `glue` blocks — not automatic,
unlike `core`-before-`glue` — would otherwise decide whether the plain `GRANT` even sees them. Pairing
both forms of the statement sidesteps that question entirely, for tables, sequences and functions
alike.

I verified this against real Postgres: a table (and its implicitly-created sequence) created
directly against the database *after* the whole `apply` finished — simulating a not-yet-written
future mutation nobody wired up to `hotel` — showed up with `~anonymous` already in `pg_class.relacl`,
no dmut run involved at all.

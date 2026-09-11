---
icon: material/history
---

# Revisions

As your database evolves, the data model changes. To avoid losing existing data, you add
incremental changes in *child* mutations instead of editing existing SQL mutations — so those
mutations are not de-applied.

Over time, definitions spread across many mutations become hard to follow. On an empty database,
it is redundant to create a table and then immediately alter it to add or remove columns.

You can consolidate by folding those changes back into the original table definition in two ways:

- **Manual rewrite**: update mutations so each `CREATE TABLE ...` includes all columns, then back
  up data, reapply mutations, and restore data. This is manageable in development but awkward in
  production when many databases or large datasets are involved.
- **Revisions**: use `__revision` and, in revisioned files, `new_needs` / `new_sql`. When a
  revision is applied, the current `needs` and `core` run as usual, but the values stored in the
  database are the `new_` ones — so the recorded source of truth becomes your cleaned-up set. New
  or empty databases are then seeded from that tidy definition.

See the [worked example](worked-example.md) for all of this against a real (throwaway) Postgres
database, or the [tutorial](tutorial.md) for a messier, more realistic table with several
children accumulated over time - new columns, a dropped one, a rename - consolidated the same way.

## `new_sql` and `new_needs` are transitory

They are only ever read when a *later* revision is applied on top: dmut reaches for the stored
`new_` values precisely when the database's revision is lower than the one being applied. So they
come in pairs — write `new_sql` in revision `n` only when you are shipping revision `n+1` that
contains the consolidated definitions those `new_` values describe.

A revision carrying `new_sql` with nothing after it is a half-finished job: nothing will ever read
what it stored. Once revision `n+1` exists and your databases have moved past `n`, the `new_`
blocks in `n` have done their work and the file can eventually be dropped.

## Revisions in your mutation files

Set `__revision: <int>` at the top level of a yaml file; every mutation in that file belongs to
that revision.

**Each revision must be self-contained.** A revision restates every mutation it consists of, not
just what changed since the previous one — `needs` cannot reach into another revision any more than
it can reach into another namespace.

Files that do not set `__revision` are treated as revision `n+1`, where `n` is the highest revision
number found *in that namespace*. If no file in a namespace sets a revision, its effective revision
is `1`.

Revisions **must** be sequential; there may be no gaps between the lowest and the highest supplied.

When you supply revisions, dmut applies every revision greater than *or equal* to the database's
current revision, in order. If the database has no revision yet, only the highest one is applied —
`dmut test -a` overrides this and replays every revision from 1, which is what you want in CI.

You do not need to keep every revision file in the codebase. In practice, keep at least the latest
revision — or a minimal file that only sets `__revision` so new databases get the right revision
number — or the lowest revision that you know is still in production.

To retire a mutation in a new revision, set `new_sql: []`. It is then recorded with an empty `core`,
so the following revision downs it as a no-op and it drops out of the graph. A mutation disappears
from `__dmut__.mutations` entirely only when it has no `core`, `glue`, `needs`, `glue_needs`,
`new_sql` and `new_needs` at all.

Use clear names for revision files, e.g. `<namespace>-r<revision>.yml` or `r<revision>.yml` — the
[worked example](worked-example.md) uses `r1.yml`, `r2.yml`, `r3.yml`.

## Considerations when writing revisions

Mutations that use "complicated" statements like `ALTER`, which cannot be auto-downed, are tricky,
and dmut makes no attempt at comparing database states: it applies, or it downs. While it does run
tests every time to catch the most common errors, it cannot catch them all. The responsibility
falls on the developer to make sure the revisions they write make sense.

You can use `glue` blocks to write unit tests in `do $$ begin ... end $$ language plpgsql`
statements, since a `raise` will fail the mutation. The [worked example](worked-example.md) uses
exactly this to assert that no data was lost across a revision.

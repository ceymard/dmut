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
- **Revisions**: use `__revision` and, in revisioned files, `delete` / `override`. `mutations:`
  still runs for real against a database coming from an earlier revision, but `override:` is what
  gets *recorded* as that revision's definition instead — so a database that starts fresh, or
  jumps straight to a later revision, is seeded from your cleaned-up set rather than replaying the
  original history of `ALTER TABLE`s. See [Squash revisions](#squash-revisions) below.

See the [worked example](worked-example.md) for all of this against a real (throwaway) Postgres
database, or the [tutorial](tutorial.md) for a messier, more realistic table with several
children accumulated over time - new columns, a dropped one, a rename - consolidated the same way.

## Squash revisions

A revision file becomes *override-shaped* the moment it declares any of `mode`, `delete`,
`override`, or `mutations` at the top level. Once it is, mutation definitions can no longer sit as
flat top-level keys — they have to live nested under `mutations:` instead, so a mutation name can
never collide with one of these four reserved words.

- **`mutations:`** is what actually runs, for real, against a database sitting at an earlier
  revision — written exactly like a plain (non-override-shaped) revision file, same `core`/`glue`/
  `needs`/`children` shape, same apply engine, same full/simple/default mode rules.
- **`override:`** is what gets *recorded* in place of the mutations named there — never executed,
  only saved, through the same load/normalise pipeline as an ordinary mutation so its hash matches
  what a later plain apply of the equivalent consolidated definition would compute. It replaces a
  mutation's `core`, `glue`, `needs`, and `glue_needs` wholesale, not just `core` — leave out a
  block you want kept and the recording of it becomes empty, not merely stale.
- **`delete:`** untracks named mutations without downing them — bookkeeping only. To actually down
  something as well as forget it, omit it from `mutations:` instead; that already downs it the
  normal way. `delete:` is for the case where a child mutation's *effect* is being folded into a
  parent's `override:`, so it doesn't need separate tracking any more, but nothing about the
  physical object should change.
- **`mode:`** (`default`, `full`, or `simple`) overrides whatever the CLI invocation asked for —
  this is how a reviewed, committed revision carries destructive intent through an automated
  pipeline that otherwise always runs `-s`.

Apply order for an override-shaped revision: `mutations:` runs for real; on success, `delete:`
names are dropped from bookkeeping and `override:` entries are saved in their place; then the
usual test pass runs — twice, actually, once against what really ran and once against the
override-swapped recording, so a mistake in `override:` (e.g. it doesn't round-trip) is caught now
rather than inherited by some future revision.

A mutation named in both `mutations:` and `override:` behaves as you'd expect: it runs for real via
`mutations:`, then what gets recorded is the `override:` version instead. A mutation disappears
from `__dmut__.mutations` entirely only when it has no `core`, `glue`, `needs`, or `glue_needs` at
all — `delete:` alone doesn't erase a row's *history*, it just stops tracking it going forward.

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

To retire a mutation in a new (plain, non-override-shaped) revision, simply omit it — an absent
mutation is downed the normal way. Inside an override-shaped revision, use `delete:` instead if the
physical object should survive (see [Squash revisions](#squash-revisions) above).

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

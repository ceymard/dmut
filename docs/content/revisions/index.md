---
icon: material/history
---

# Revisions

As your database evolves, the data model changes. Dmut lets you keep adding statements at the end of
a mutation, or new child mutations, so that nothing that already ran is taken down.

Over time, definitions spread across many statements and mutations become hard to follow. On an empty
database, it is redundant to create a table and then immediately alter it to add or remove columns.

You can consolidate by folding those changes back into the original table definition in two ways:

- **Manual rewrite**: update mutations so each `CREATE TABLE ...` includes all columns, then back
  up data, reapply mutations, and restore data. This is manageable in development but awkward in
  production when many databases or large datasets are involved.
- **Revisions**: use a revision document with `revision`, `delete` and `override`. `mutations:`
  still runs for real against a database coming from an earlier revision, but `override:` is what
  gets *recorded* as that revision's definition instead — so a database that starts fresh, or
  jumps straight to a later revision, is seeded from your cleaned-up set rather than replaying the
  original history of `ALTER TABLE`s. See [Squash revisions](#squash-revisions) below.

See the [worked example](worked-example.md) for all of this against a real (throwaway) Postgres
database, or the [tutorial](tutorial.md) for a messier, more realistic table with several
children accumulated over time - new columns, a dropped one, a rename - consolidated the same way.

## Squash revisions

A revision document is a yaml document whose first key is `revision`. Its mutations live nested under
`mutations:`, and it may carry these keys:

- **`mutations:`** is what actually runs, for real, against a database sitting at an earlier
  revision — a map from mutation name to mutation, with the same keys as a mutation file.
- **`override:`** is what gets *recorded* in place of the mutations named there — never executed,
  only saved. It replaces a mutation's `sql`, `structural`, `deps` and `reapply_on` wholesale: leave
  out a key you want kept and the recording of it becomes empty or `false`. Restate the seeds you
  want kept, too.
- **`delete:`** forgets named mutations without downing them — bookkeeping only. To actually down
  something as well as forget it, omit it from `mutations:` instead; that already downs it the
  normal way. `delete:` is for the case where a child mutation's *effect* is being folded into a
  parent's `override:`, so it doesn't need separate tracking any more, but nothing about the
  physical object should change. A name in both `delete:` and `override:` is deleted.
- **`mode:`** (`safe`, `normal` or `force`) overrides whatever the command line asked for —
  this is how a reviewed, committed revision carries destructive intent through an automated
  pipeline that otherwise always runs `-s`.

Apply order for a revision with `override:` or `delete:`: `mutations:` runs for real; on success,
`delete:` names are dropped from bookkeeping and `override:` entries are saved in their place; then
the usual test phase runs — twice, actually, once against what really ran, and once against the
override entries as recorded, so a mistake in `override:` (an entry that doesn't round-trip, or one
that names an object that isn't there) is caught now rather than inherited by some future revision.
The second pass tests only the override entries: the mutations that `delete:` forgot are no longer
part of what is tested.

A mutation named in both `mutations:` and `override:` behaves as you'd expect: it runs for real via
`mutations:`, then what gets recorded is the `override:` version instead. A name that is only in
`override:` is recorded without running, which adopts an object that already exists.

Applying a revision the database is already at compares the files with what was recorded: the
`override:` versions and without the `delete:` names, not the `mutations:` that ran. So applying the
same files twice does nothing the second time.

`dmut collect -r <n>` writes a revision shell to start from, and `dmut collect` writes revision
documents back out. See [Collecting and exploding](../mutation-format/collect-explode.md).

## Revisions in your mutation files

A revision document starts with `revision: <int>` and every mutation in it belongs to that revision.
A mutation *file* (one mutation, named after the file) has no revision key: it is at the unnamed
revision, which is the one after the highest numbered revision of its namespace, or `1` when there is
none.

**Each revision must be self-contained.** A revision restates every mutation it consists of, not
just what changed since the previous one — `deps` cannot reach into another revision any more than
it can reach into another namespace.

Revisions **must** be sequential; there may be no gaps between the lowest and the highest supplied.

When you supply revisions, dmut applies every revision greater than *or equal* to the database's
current revision, in order. If the database has no revision yet, only the highest one is applied —
`dmut test -a` and `dmut apply -a` replay every revision from the first, which is what you want in CI.

You do not need to keep every revision file in the codebase. In practice, keep at least the latest
revision — or a minimal document that only sets `revision` so new databases get the right revision
number — or the lowest revision that you know is still in production.

To retire a mutation in a new revision, simply omit it — an absent mutation is downed the normal
way. Inside a revision with `override:`, use `delete:` instead if the physical object should survive.

Use clear names for revision files, e.g. `<namespace>-r<revision>.yml` or `r<revision>.yml` — the
[worked example](worked-example.md) uses `r1.yml`, `r2.yml`, `r3.yml`. The name of a file with a
revision document means nothing to dmut.

## Considerations when writing revisions

Mutations that use "complicated" statements like `ALTER`, which cannot be auto-downed, are tricky,
and dmut makes no attempt at comparing database states: it applies, or it downs. While it does run
tests every time to catch the most common errors, it cannot catch them all. The responsibility
falls on the developer to make sure the revisions they write make sense.

You can use [`ensure:`](../mutation-format/statements.md#checks) statements, in a default mutation
that depends on the table, to write unit tests for a revision: a failed check fails the apply. The
[worked example](worked-example.md) uses exactly this to assert that no data was lost across a
revision.

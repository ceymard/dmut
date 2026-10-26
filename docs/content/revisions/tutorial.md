# Tutorial: consolidating years of `ALTER TABLE`

The [worked example](worked-example.md) shows revisions doing the one thing they're for, in the
smallest possible shape: one column, added after the fact, later folded back into `CREATE TABLE`.
Real tables rarely stop there. This walks through a `customers` table that picked up five separate
changes over what a comment in the example calls "years" - new columns, a column that got dropped
after a data migration, and a rename - each written the way it would actually have happened, one
small mutation at a time, without editing anything that came before it. Then it gets consolidated,
the same way the worked example's table did.

Every statement below has actually been run, the same way as the worked example: `dmut test -a
examples/revisions-tutorial` (replaying revision 1 → 2 → 3 against a throwaway Postgres container),
`dmut test examples/revisions-tutorial` (a fresh database landing directly on revision 3), and three
real `dmut apply` calls against a running Postgres, inspected with `psql` after each one.

## Revision 1: five years of `ALTER TABLE`, as they actually happened

```yaml
--8<-- "examples/revisions-tutorial/r1.yml"
```

Nothing here is folded together. `customers` is the table as it was first created; every column
change since is its own child mutation, named for what it did rather than for some object it
introduced, because that's genuinely what these are - patches, not new sub-objects. Two things
are worth calling out that don't show up in the smaller worked example:

- **Ordering between siblings isn't automatic.** A dotted name only ever depends on its own
  ancestors - `customers.retire_phone` does **not** automatically run after `customers.phone` just
  because they're both children of `customers`. Since `retire_phone` drops a column that `phone` is
  what added, it lists `deps: [customers.phone]`, or it can run first and fail against a database
  that never had a `phone` column to begin with. Every other child here is independent of its
  siblings and needs nothing extra.
- **Not everything auto-downs.** `split_name`'s backfill is a plain `UPDATE`, which dmut has no
  opinion on how to reverse, and the `DROP COLUMN` right after it isn't safe for dmut to guess a
  down for either (see [Automatic down statements](../mutation-format/auto-down.md)). Each is an
  explicit `up`/`down` pair. Because the downs of a mutation run in the *reverse* order of its `sql`
  list (add `first_name`, add `last_name`, backfill, drop `full_name` → undo runs the drop's down
  first), downing this mutation genuinely reconstructs `full_name` from the two new columns before
  dropping them - not just a hand-wavy mirror of the schema change.

## Revision 2: telling dmut what the cleanup looks like

```yaml
--8<-- "examples/revisions-tutorial/r2.yml"
```

This revision declares `delete` and `override` (see [Squash revisions](index.md)). `mutations:` here
changes nothing about what runs against a database already sitting at revision 1 - every mutation is
restated exactly as it is in `r1.yml`, because a revision restates every mutation it consists of,
not a diff against the one before.

The two additions are `override.customers` (the fully consolidated table, plus the same seed data
reshaped to the final column names) and `delete`, listing all five children by name to retire them
from tracking once revision 3 lands - they already ran for real via `mutations:`, so once their
combined effect is folded into `customers`'s override, they no longer need separate rows.

`customers`'s override has to match revision 3's `customers` exactly - a database that already has
revision 2's override recorded and a database that lands on revision 3 directly need to agree on
exactly what they built. It's produced by mentally replaying every child's effect against the
original `CREATE TABLE`: `full_name` gone, `first_name`/`last_name`/`marketing_opt_in` added,
`signed_up_at` renamed to `created_at`, and `phone` never shows up at all, because it was both added
and dropped inside this same revision. `override` replaces a mutation's recorded statements and keys
wholesale, so leaving anything out doesn't fail loudly - it just makes the *recorded* version
shorter.

## Revision 3: the file that stays

```yaml
--8<-- "examples/revisions-tutorial/r3.yml"
```

None of the five child mutations are restated here - they don't need to be. Revision 2's `delete`
already told dmut they drop out of tracking once revision 3 lands - not downed, just forgotten,
since their effect lives on inside `customers`'s override. This is also the file worth keeping in the
repository long after `r1.yml` and `r2.yml` could be deleted: it's the one a brand-new database
actually applies, and the one that reads like the table always looked this way.

`customers.revision_check` is a unit test for the revision itself, in the same spirit as the worked
example's: its `ensure:` statements fail the whole apply if the two seeded rows didn't survive the
trip through revision 2, if the name split lost data, or if `full_name`, `phone` or `signed_up_at`
are somehow still around.

## Does this feel clunky?

Three things stood out while building this, worth naming plainly rather than leaving implicit:

1. **Revision 2 is mostly a copy of revision 1.** For five children, that's five mutations
   duplicated verbatim under `mutations:`, with only a `delete` entry added for each. That
   duplication is deliberate - "each revision must be self-contained" is the whole safety property
   that lets dmut apply any single revision on its own - but it does mean a table with many
   incremental children turns revision 2 into a large, mostly-unchanged file. There is no "inherit
   from the previous revision unless overridden" shortcut.
2. **`override` must be produced by hand, and only checked at apply time.** Nothing statically
   checks that revision 2's `override.customers` actually matches what revision 3's `customers` will
   declare - dmut discovers a mismatch when revision 3 is applied on top of a database that already
   carries revision 2's override, which forces an unwanted drop-and-recreate instead of raising a
   clear "these don't match" error ahead of time. Mentally replaying every child's effect to write
   that `override` by hand is exactly the kind of bookkeeping a person gets wrong. `dmut test -a`
   catches it (the mismatch shows up as an unexpected down and up of `customers` in the revision 2 →
   3 transition), but only after the fact, in CI, not while writing the file.
3. **The seed is restated in every version.** It sits right after the `create table`, in the same
   mutation, so that it runs before the children that alter or drop columns, and it has to be
   rewritten for the final column names in the override.

None of these are bugs; they're the direct, honest cost of dmut's guarantee that every revision is
independently applicable. For a table with one or two incremental changes (the worked example's
shape) the cost is invisible. It shows up specifically when a table accumulates many small children
before its first consolidation, which is exactly the case this tutorial was built to exercise.

## Running it yourself

```sh
dmut test -a ./examples/revisions-tutorial            # replay 1 → 2 → 3 against a throwaway Postgres
dmut test ./examples/revisions-tutorial                # a fresh database only sees revision 3

dmut apply postgres://user:pass@localhost/mydb ./examples/revisions-tutorial/r1.yml
dmut apply postgres://user:pass@localhost/mydb ./examples/revisions-tutorial/r1.yml ./examples/revisions-tutorial/r2.yml
dmut apply postgres://user:pass@localhost/mydb ./examples/revisions-tutorial
```

# Dmut changes

A few changes are needed to make dmut nicer to use.

## Renaming

`sql` and `meta` are not clear enough in their intent. `core` and `glue` will replace them everywhere, to better convey their meaning. `meta_needs` becomes `glue_needs` to require mutations' glue be run before this one. `needs` itself keeps its name - it already paired with `meta_needs` before this rename, and stays the shorter, more common key.

## Behavioural changes

The rules around mutations being downed change : they are no longer monolithic blocks of statement that are atomically deleted and recreated.

`needs` dependencies still indicates running order for mutations, but not necessarily down/up of all children of a changing mutation.

## New algorithm

If a `core` change actually requires a real down - i.e. it has a non-empty _old_ segment, not just appended statements - then all the `glue` of all mutations in the namespace is downed and re-upped, exactly as before. A pure append to any `core` never triggers this.

Mutations are considered by order of parentality and go by roots first and most dependents last. We start with `core` first and run the algorithm below on it. If the rule above did not already trigger a full teardown of `glue` (i.e. no `core` anywhere needed a real down), the same algorithm then runs on `glue` too, per mutation - so an isolated function-body or grant edit doesn't force a rebuild of unrelated glue either.

When applying mutations, the runner compares the statement arrays of each mutation with their database-stored counterpart. The comparison is on code-equivalence (whitespace and comments normalized away), not exact string matching, and it is order-sensitive: it finds the longest _common prefix_ of the two arrays, not a general diff. From that it considers:
- the _common_ start of the two statement arrays
- the _old_ end of the currently applied mutation - whatever follows the common prefix in what's recorded
- the _new_ end of the incoming mutation - whatever follows the common prefix in what's being applied

Because this is a prefix match rather than a full diff, editing a statement in the middle of an otherwise-unchanged list tears down and redoes everything after that point, even if later statements are byte-for-byte identical to before. This is not just a churn-minimization trade-off, it's required for correctness: a later statement can depend on what an earlier one produced (e.g. it renames a column the earlier statement added), so leaving it untouched while the earlier one changes underneath it would either leave its old, now-orphaned effect in place, or make the earlier statement's own down impossible to run against the object's current (renamed) state. Including everything after the divergence point in the down/up pair - downed in reverse order, so a later rename is undone before the earlier create/add it depended on - is what keeps both directions consistent. If the later statement's text was never updated to match the earlier change (the author renamed something in one statement but forgot to update a later statement that referenced the old name), that mismatch now surfaces as a loud, immediate error on the up side, instead of staying silently stale.

It will try, in a savepoint, to just down the _old_ end and up the _new_ end. What happens if that fails depends on the apply mode (see "Apply changes" below):
- under `full` mode, it downs all children mutations recursively, leaves first, and retries - those children then have to redo _all_ their statements;
- under `default` mode, it stops without cascading to children;
- `simple` mode never gets this far for a mutation with a non-empty _old_ segment - it refuses upfront instead (see below).

The test pass does not change, and this is load-bearing, not incidental: it must always down-then-up every mutation's _entire_ `core`/`glue` array from nothing, unconditionally, for every mutation in the namespace - never selectively, based on whether that mutation's own array individually looks changed. This is what makes the whole scheme safe. A statement whose content genuinely didn't change (e.g. a later `add column` left untouched while an earlier `create table` was edited) can still have its *physical effect* destroyed as collateral by something else that changed nearby (`drop table` takes a column with it, `drop column` takes a plain index with it, silently, no `cascade` needed, no error) - the real apply alone would leave that undetected. The exhaustive test pass catches it regardless, because it re-derives every mutation's down statements from the current full definitions and runs them against the real, live target database in the same transaction, independent of whatever subset the real apply's own old/new-tail logic touched. If the test pass were ever "optimized" to also skip statements that individually look unchanged, this collateral-damage class of bug would become permanently invisible - there is no other net that catches it. Longest-common-prefix (rather than a true diff) for the *real apply* is what avoids being fully dependent on this invariant in the first place, since it conservatively sweeps everything positionally after a divergence back into scope rather than trying to recognize "unchanged" content - but the exhaustive test pass is the one guarantee that must never be relaxed, on either algorithm.

Why : when we have just, for instance, a `create table`, we don't want if we add `create index` to the core statements to have to down the table first. We also actually don't want to create new children mutations just for those statements, this is untidy.

## Apply changes

`dmut apply` changes behaviour and operates in three distinct levels :
- full mode (-f / --full) : changes are tried first without downing dependants but downs them if the old/new mechanic cannot be applied simply
- simple mode (-s / --simple) : dmut stops if core has any _old_ segments at all ; only run if the only changes are _added_.
- default mode : dmut downs the _old_ and ups the _new_, but will not pursue dependent downing if a statement fails. This is what runs when no flag is given.

A revision can declare its own required mode via `mode:`, which then wins outright over whatever the CLI invocation asked for (see "Squash revisions" below) - this is deliberate: it's how a reviewed, committed revision carries destructive intent through an automated pipeline that otherwise always runs `-s`.

## "Squash" revisions

This part entirely change revisions. A revision is still considered by its sequence number. It is still possible to remove obsolete revisions. Unseeded databases still use the latest revision (usually the un-numbered, current one.)

Users are encouraged to keep the un-numbered/current one as current truth ; numbered revisions are ONLY ever useful for overrides.

A revision is still recognised the same way it is today: by the presence of `__revision` at the top level of the file. A revision file additionally has the following top-level keys available :

- `__revision` : the sequence number of the revision
- `__namespace` : optional namespace
- `mode` : the apply mode (`default` | `full` | `simple`) this revision must run under. Declaring it here overrides whatever mode the CLI invocation was given.
- `delete` : a `[]string` containing the names of mutations to be deleted from the dmut table. Optional. This only removes bookkeeping - it does **not** run a down for them. To retire a mutation for real (down it and stop tracking it), simply omit it from `mutations` instead: dmut sees it's no longer present and downs it the normal way before dropping it from tracking.
- `override` : the new, overriding definition of mutations. This section is mainly used to fold/squash together a mutation that has been using a lot of `alter table` back into a single `create table`.
- `mutations` : the mutations as they will be _applied_, given the database as it currently is - written exactly like an ordinary (non-revisioned) mutation file.

The presence of any of `mode` / `delete` / `override` / `mutations` marks a revision as override-shaped, which is what requires mutation definitions to live nested under `mutations:` instead of as flat top-level keys the way a plain revision file uses today - this keeps a mutation that happens to be named e.g. `delete` from ever colliding with these reserved keys.

A revision can be scattered across several files, which are then merged together by name and sequence, although this is not recommended for git tracking reasons.

When applying a revision :
- dmut starts by applying what's in `mutations` just like if it were the current unnamed revision - under the *same* apply mode (`full` / `default` / `simple`) as any other apply, unless this revision's own `mode:` overrides it. There is no special-cased hard-abort-on-mismatch behavior: a genuine drift between `mutations` and what's actually recorded is handled exactly like any other unexpected `core` change would be, under whatever mode is in effect. This is deliberate - the goal is only to stop users being *surprised* by data-deleting changes, not to forbid them outright. Automated/production deployments should run under `-s` (simple) by default specifically so drift is refused rather than silently reconciled, and a revision that genuinely needs to reconcile or replace history declares `mode: full` itself.
- If there are no errors, `delete` mutations are removed from dmut's tracking table, and `override` mutations are saved into the table. `override` entries go through the exact same load/normalise pipeline as an ordinary mutation's `core`/`glue` (so their recorded hash is computed identically to what a later, unrelated apply would compute from the same text) - they are just never executed, only recorded, since the real apply already brought the database to this shape via `mutations`. This is what lets a subsequent, ordinary apply see them as already up to date and do nothing.
- The usual tests are run

As before, revisions are applied in order. The unnumbered revision (current code) is ALWAYS considered as `revision` + 1, although it is never saved as such. It is the "living code."

## Changes to collect

Collect should be used to create revisions. It should accept the flag `-r|--revision <number>` that will create a revision with

```yaml
__revision: <number>
__namespace: <namespace>

mode: default # the mode this mutation must run in, overrides what apply says and is a deliberate decision. Can be default | full | simple

# names of mutations to lose track of
delete: []

# new definitions of mutations that will override those in place
override:
  # ...

mutations:

  mutation.one:
    core: ...
    glue: ...
    
```

`-r` can source `mutations:` from the paths you already pass it today (the primary, intended use: collect your current, already-accumulated local files into a revision shell so you can write `override` by hand) - or, alternatively, from a live database given as a postgres URI, which just downloads whatever's recorded there straight into the file. The URI form only exists for the case where you have no local files at all (an inherited/orphaned database) - it is explicitly not the primary workflow.

`-r` checks when called against a database that the user gave `n+1` and warns if they didn't. Locally, if revisions were in the paths that were given to collect, it also checks that the user entered `n+1`

# How a run works

Everything happens inside a single transaction, per namespace, in ascending revision order.
Failure at any step halts the process and nothing is applied.

1. Read the mutations already applied from `__dmut__.mutations` for that namespace.
2. Compute the delta. A mutation is obsolete if it no longer exists locally or if its hash changed.
   Obsolete mutations are downed along with everything that depends on them, recursively.
3. Down the obsolete `glue`, down the obsolete `core`, up the new `core`, then up the new `glue`. If
   *any* `core` had to be downed, *all* `glue` in the namespace is downed and re-upped, since glue
   may depend on any object the core touched. That rebuild is logged as two records, not one per
   glue mutation (see [Logging](cli-reference.md#logging)).
4. Record the new state in `__dmut__.mutations`.
5. Test, when anything changed: down all `glue`, then replay each mutation's `glue` chain
   up-then-down on its own; then down all `core` and do the same for `core`. This runs against the
   target database behind savepoints and is rolled back afterwards, so no data is lost — but every
   mutation must be cleanly downable or the whole apply aborts.
6. Commit, or roll back if `-d` was given.

A revision that defines `new_sql` / `new_needs` is additionally tested in its "new" form, so that
the definitions the *next* revision will inherit are known to be valid before anything depends on
them. A database pays for that pass on the run that carries it across that revision, and never
again — see [Revisions](../revisions/index.md).

`dmut test` runs the same sequence against a throwaway Postgres container and never commits.

## Watching it happen

Run any of the examples in this documentation with `-v` against a real database (or
`dmut test -v` against a throwaway one) to see this sequence directly — every statement dmut runs,
in order, gets echoed. For instance, applying [`examples/hotel`](
https://github.com/ceymard/dmut/tree/main/examples/hotel) for the first time prints every `core`
statement (schema, types, tables, indexes) before a single `glue` statement (comments, functions,
grants) runs, exactly as the sequence above describes.

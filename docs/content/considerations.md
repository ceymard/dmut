---
icon: material/alert-decagram
---

# Considerations

- **Do not use `CREATE ... IF NOT EXISTS` or `DROP ... IF EXISTS`.** Dmut is meant to own the whole
  life cycle of a symbol in the database: if a statement silently succeeds whether or not the
  object is there, a botched down goes unnoticed and the test phase can no longer catch it. Dmut
  will parse and auto-down `CREATE ... IF NOT EXISTS` without complaining — that leniency is there
  for the case where the database already contains a symbol and you are adding dmut on top of it.
  Outside that case, don't. (`DROP ... IF EXISTS` has no auto-down at all; no bare `DROP` does.)

- **Never put `CASCADE` in a `DROP` statement** in your custom mutations. Dmut relies on every
  object being created in its own mutation with dependencies declared explicitly. For your safety,
  it must break during the test phase when they were not.

- **Every mutation must be downable.** The test phase downs and re-ups everything; a mutation that
  cannot be undone will fail the whole apply, not just itself. See [Automatic down statements](mutation-format/auto-down.md) for the handful of statements where "downable" means writing an
  explicit `down` yourself.

- `needs` and `meta_needs` may not cross a [namespace](mutation-format/namespaces.md) or a
  [revision](revisions/index.md).

- **Write `new_sql` only alongside the next revision.** See [Revisions: `new_sql` and `new_needs`
  are transitory](revisions/index.md#new_sql-and-new_needs-are-transitory).

None of these are enforced by clever tooling beyond what's already described elsewhere in this
documentation — they're consequences of how dmut's dependency model and test phase actually work,
and violating them tends to surface as a confusing failure during `apply` or `test` rather than a
clear upfront error.

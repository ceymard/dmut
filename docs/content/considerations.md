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
  `ALTER TYPE ... ADD VALUE IF NOT EXISTS` is the exception: it is a [seed](
  mutation-format/statements.md#seeds), which is meant to be safe to run more than once.

- **Never put `CASCADE` in a `DROP` statement** in your custom mutations. Dmut owns the whole
  lifecycle of what it creates, and relies on each object being created in its own mutation with its
  dependencies declared. A `DROP ... CASCADE` silently removes everything that depends on the
  object, including things dmut does not know are affected: dropping a view with `CASCADE` also
  drops any materialized view built on it, and with it the data. Without `CASCADE`, Postgres
  refuses to drop an object something still depends on, and the test phase reports the missing
  dependency. Dmut does not check for this; it is on you.

- **Every mutation must be downable.** The test phase downs and re-ups everything; a mutation that
  cannot be undone will fail the whole apply, not just itself. See [Automatic down
  statements](mutation-format/auto-down.md) for the handful of statements where "downable" means
  writing an explicit `down` yourself.

- **Every seed must be safe to run twice.** The test phase runs each seed twice in a row, and a seed
  runs again every time its part of the mutation goes up. Use `on conflict do nothing`, `where not
  exists` or an upsert.

- **List in `deps` every mutation whose objects your statements use.** A foreign key to another
  table, a column of a type created elsewhere, a function called by a view. The test phase
  reapplies each mutation with its dependents and fails, naming the mutation and the statement,
  when something cannot go down because it is used by a mutation that did not say so.

- **Say which events should reapply a default mutation.** A view that reads a table is reapplied
  when the table is reapplied, which a structural table never is by itself. If the view must follow
  the columns that are appended to the table, add `reapply_on: [dep_reapplied, dep_appended]`. See
  [Dependencies and reapply triggers](mutation-format/dependencies.md).

- **A grant "on all tables" only covers what exists when it runs.** Make the grants mutation depend
  on the schema with a pattern (`hotel.**`) and `dep_appended`, as the [example
  database](example-database/walkthrough.md#grants-that-follow-the-schema) does, so that it runs
  again when the schema grows.

- **A statement that is not on the default list belongs in a structural mutation.** Dmut tells you
  when you get it wrong, when the files are loaded.

- `deps` may not cross a [namespace](mutation-format/namespaces.md) or a [revision](revisions/index.md).

- **Never save a secret.** A [saved setting](mutation-format/settings.md#settings-in-downs) is
  stored in clear in the database. A tracked value is only hashed, which does not protect a weak
  secret: use `tracked: false` for one.

None of these are enforced by clever tooling beyond what's already described elsewhere in this
documentation — they're consequences of how dmut's dependency model and test phase actually work,
and violating them tends to surface as a confusing failure during `apply` or `test` rather than a
clear upfront error.

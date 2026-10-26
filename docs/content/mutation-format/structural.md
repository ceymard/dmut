# Structural and default mutations

A mutation is either *structural* or, by default, not. The difference is what dmut may do with it
on its own.

A **structural** mutation creates or alters things whose removal loses data or is slow to redo:
tables, columns, indexes, types, schemas, extensions, roles. It is declared with `structural: true`
and it is never reapplied automatically unless it asks (`reapply_on` is empty by default).

A **default** mutation describes behaviour and access: functions, views, triggers, policies, grants,
comments. It can be taken down and brought back up as often as needed without losing a single row,
so dmut reapplies it by itself whenever something it depends on is reapplied (`reapply_on` is
`[dep_reapplied]` by default). See [Dependencies and reapply triggers](dependencies.md).

## What a default mutation may hold

dmut checks this when the files are loaded, before it connects to a database. A default mutation
accepts only:

- `CREATE [OR REPLACE]` / `DROP` / `ALTER` (including `OWNER TO` and renames) of a `FUNCTION`,
  `PROCEDURE`, `AGGREGATE`, `VIEW`, `TRIGGER` (including `CONSTRAINT TRIGGER`), `POLICY` or `RULE`
- `CREATE TYPE name AS (...)` and `DROP TYPE`: composite types. A `DROP TYPE` cannot be checked for
  that, so dmut accepts it; do not use it on an enum.
- `ALTER TABLE <table> ...`, but only these actions, any number of them comma-separated:
    - `ENABLE` / `DISABLE` / `FORCE` / `NO FORCE ROW LEVEL SECURITY`
    - `ENABLE` / `DISABLE TRIGGER` (with `REPLICA` or `ALWAYS`)
    - `ALTER COLUMN <name> SET DEFAULT ...` and `ALTER COLUMN <name> DROP DEFAULT`
- `GRANT`, `REVOKE` and `ALTER DEFAULT PRIVILEGES`
- `COMMENT ON ...`
- statements that read or write rows or run code: `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE`,
  `COPY`, `SELECT`, `WITH`, `DO` and `CALL`. A default mutation is reapplied automatically, so the
  author owns the cost and the effect of these.

Everything else is refused with a message that names the file, the mutation, the position of the
statement and its first words, and tells you to set `structural: true`. The body of a
`CREATE FUNCTION ... $$ ... $$` and of a `DO` block is not inspected. Both the `up` and a hand-written
`down` are checked. A statement string holding several statements is checked statement by statement.

A **structural** mutation accepts every statement, except that nothing may start, end or nest the
transaction, in any mutation: `BEGIN`, `START TRANSACTION`, `COMMIT`, `END`, `ROLLBACK`,
`SAVEPOINT`, `RELEASE`, `PREPARE TRANSACTION`. Dmut owns the transaction.

## Changing the flag

Changing `structural` on a mutation that is already applied changes no statement and runs nothing.
Dmut records the new flag, the new statements must pass the check of the new kind, and the default
`reapply_on` changes from the next run on. The change is accepted in the modes `normal` and `force`,
and refused in `safe`.

## Reapplying a structural mutation

To reapply a mutation is to take it down in full and bring it up in full. For a structural mutation
that drops what it created, with the data. That is why the default is to never do it automatically,
and why the mode `safe` refuses it. If you do want a structural mutation to follow a dependency,
say so with `reapply_on`.

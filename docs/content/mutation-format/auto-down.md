# Automatic down statements

For most statements, the undo can be inferred, so you write a plain string instead of an
`up`/`down` pair. The inferred undo is always the destructive counterpart (`CREATE` → `DROP`,
`GRANT` → `REVOKE`, `ADD COLUMN` → `DROP COLUMN`, …).

Dmut does **not** query the database to guess what the previous state was, which is why many
`ALTER` statements cannot be auto-downed and need an explicit `down`.

Auto-down covers `CREATE` for around thirty object kinds (table, index, view, materialized view,
schema, type, domain, role, function, procedure, aggregate, trigger, policy, rule, sequence,
extension, operator, operator class/family, cast, collation, conversion, language, publication,
subscription, server, statistics, tablespace, text search objects, transform, user mapping,
foreign table, foreign data wrapper, access method, event trigger), the common `ALTER TABLE`
forms, `GRANT`, and `ALTER DEFAULT PRIVILEGES`.

[`mutations/model/test/auto-down-tests.yml`](
https://github.com/ceymard/dmut/blob/main/mutations/model/test/auto-down-tests.yml) is the authoritative
list: every supported form appears there with the down dmut generates for it.

A plain string must be a single SQL statement. If dmut cannot infer a down, loading the file fails
with an error — write the `up`/`down` pair yourself, or `seed:` if there is nothing to undo. In
particular:

- A bare `DROP ...` never auto-downs. Dmut only understands `CREATE`, `ALTER TABLE`, `GRANT`,
  `ALTER DEFAULT PRIVILEGES` and `COMMENT ON`.
- `COMMENT ON` has no down at all — dmut leaves the comment in place rather than guess what it used
  to be. It is still written as a plain statement, and dmut treats it as a [seed](statements.md#seeds);
  "no down" and "we could not find a down" are different outcomes, and only the second one is an
  error.
- `ALTER TABLE ... ATTACH PARTITION` auto-downs to `DETACH PARTITION`. The reverse,
  `DETACH PARTITION`, does not auto-down: reattaching needs the original `FOR VALUES` bounds, which
  aren't present in the `DETACH` statement itself, so it needs an explicit `down`.
- `ALTER TYPE ... ADD VALUE` (adding an enum value) can never be auto-downed, not just today but in
  principle: Postgres has no way to remove a value from an enum type short of rebuilding the type,
  which is destructive if anything already stored that value. With `IF NOT EXISTS` dmut takes it as a
  [seed](statements.md#seeds), which is safe to run more than once and is left alone when the
  mutation goes down. Without it, write an explicit `down` — typically a type rebuild — or the
  `seed:` prefix.
- `CREATE INDEX CONCURRENTLY` (and other `CONCURRENTLY` variants) will auto-down and load fine, but
  fails at execution: Postgres refuses to run `CONCURRENTLY` statements inside a transaction block,
  and every dmut run is transactional. There's no way to make this work under dmut's execution
  model — use a plain `CREATE INDEX` instead.

`CREATE FUNCTION`/`PROCEDURE`/`AGGREGATE` auto-downs correctly even with `OUT`, `INOUT` and
`VARIADIC` parameters: the generated `DROP` signature keeps `IN`, `INOUT` and `VARIADIC` parameter
types (the ones Postgres actually matches on when identifying the function to drop) and leaves out
`OUT`-only ones. See [`hotel.split_name`](../example-database/walkthrough.md) in the example
database for a function that relies on this.

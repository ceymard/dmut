# Doc comments

A comment that starts with `--?`, or a block comment that starts with `/*?`, is a **doc comment**.
Every other comment is ignored, as before. A doc comment is kept, and becomes a `COMMENT ON`
statement for the object it sits in front of.

```yaml
sql:
  - |
    --? One row per booking.
    create table bookings (
      --? The booking number. Never reused.
      id int primary key,
      /*? Who booked.
          Guests without an account are stored by email. */
      guest text not null,
      --? A booking cannot end before it starts.
      constraint bookings_dates check (ends_on >= starts_on),
      starts_on date,
      ends_on date
    )
```

dmut reads it as the table, then one `COMMENT ON` statement for the table, one for `id`, one for
`guest` and one for the constraint.

## Where a doc comment goes

The comment goes **right before**:

- a `CREATE` of anything Postgres can comment on: tables, views, materialized views, foreign
  tables, types, domains, schemas, sequences, indexes, functions, procedures, aggregates,
  operators, operator classes and families, triggers, policies, rules, extensions, publications,
  subscriptions, servers, foreign data wrappers, text search objects, and so on. `CREATE USER
  MAPPING` cannot be commented.
- a column of `CREATE TABLE`, `CREATE FOREIGN TABLE` or `CREATE TYPE ... AS (...)`, or a name in
  the column list of `CREATE VIEW v (a, b)` and `CREATE MATERIALIZED VIEW`.
- a **named** constraint, either on its own line in a table (`constraint name ...`) or in the middle
  of a column definition.
- an `ALTER TABLE ... ADD COLUMN`.

Several `--?` lines in a row, or a blank line between the comment and its target, are fine: stacked
lines are joined with a newline. A `--?` with nothing after it is an empty comment, which removes
the comment from the object in Postgres.

A doc comment anywhere else is an error that names its line, so a misplaced comment is never
dropped quietly. That includes a constraint without a name (Postgres picks the name, and which one
depends on what already exists in the database: write `constraint some_name ...` to comment it), an
enum value, a function parameter, a partition, and anything inside a `$$ ... $$` body, where
`--?` is just an ordinary comment. Doc comments only work in plain strings and in `auto:` statements.
A block doc comment does not nest.

## When the comments run

The `COMMENT ON` statements are added **after all the other statements of the mutation**, in the
order they were written, whatever statement they were written in. The statements are of the kind
`doc`: they run every time they go up, and do nothing when they go down.

That makes them a tail that can change freely:

- Editing, adding or removing a doc comment does not change the statement it sits in: its hash
  ignores doc comments, like all comments. Only the doc statements go up again.
- That is true in the mode `safe` as well, and it does not make the mutations that depend on it
  reapply, not even those that wait for `dep_appended`.
- A statement added at the end of the mutation goes in front of the doc statements, and counts as an
  append.

The comment text is stored in the doc statement, so [`dmut collect`](collect-explode.md) writes
the comments as plain `COMMENT ON` statements at the end of the mutation, not as doc comments.

One thing to keep in mind: the comments run last, with the names written in the `CREATE`. If a later
statement of the same mutation renames the object or drops the column, the `COMMENT ON` fails with
Postgres's own error. Put the doc comment on the statement that creates the final shape.

# Statements, seeds and checks

The `sql` list of a mutation holds statements. A statement is a plain string, or a mapping with
exactly one of `auto`, `up`, `seed` or `check`, and an optional `settings` (see
[Settings](settings.md)). `data` is allowed with `seed` only, and `down` with `up` only.

| Form | Meaning |
|---|---|
| plain string | A reversible statement. Dmut infers the down. It must be a single SQL statement, and dmut must be able to infer its down. |
| `auto: <sql>` | Same as a plain string. It exists so that the statement can carry `settings`. |
| `up: <sql>` with `down: <sql>` | A reversible statement with an explicit down. The `down` may not be empty, and may hold several SQL statements. |
| `seed: <sql>` | A seed: no down. It may carry `data`. |
| `check: <sql>` | A check: no down. |

A `COMMENT ON ...` statement has nothing to undo: it is a doc statement, which runs like a seed
(see [Doc comments](doc-comments.md)). `ALTER TYPE ... ADD VALUE IF NOT EXISTS ...` is a seed
without writing `seed:`. `ALTER TYPE ... ADD
VALUE` without `IF NOT EXISTS` has no automatic down: write an explicit one, or `IF NOT EXISTS`, or
`seed:`.

## Seeds

A seed is for statements with no way back, typically reference data.

- A seed runs each time its mutation goes up, and each time the tail that contains it goes up.
- A seed is skipped when its mutation goes down, and when the tail that contains it comes down.
- A seed before the first changed statement does not run again when the tail changes.
- A seed may appear in any mutation, structural or not.
- **The author makes a seed safe to run more than once** (`on conflict do nothing`, `where not
  exists`, an upsert). The test phase runs each seed twice in a row, so a seed that is not safe fails
  there.

```yaml
structural: true
sql:
  - create table roles (id int primary key, name text not null, tags jsonb)
  - seed: insert into roles (id, name, tags) values ($1, $2, $3) on conflict (id) do nothing
    data:
      - [1, admin, '["a", "b"]']
      - [2, guest, null]
```

### Data

`data` is a list of rows, a row being a list of values. The statement uses the parameters `$1` to
`$n`, and `n` must equal the length of every row. Dmut runs the statement once for each row, in
order, in one batch in the same transaction. With no row, the statement does not run. A statement
with `data` must be a single SQL statement.

Each value is sent as the text it is written as in the file, and Postgres reads it for the type of
the column, as `COPY` does. A YAML `null` is `NULL`. A value that is a map or a list is sent as JSON
text. If you want a JSON document in one parameter, write a row with one map: `data: [[{a: 1}]]`
and `$1::jsonb`. YAML anchors and aliases work inside one document.

A change of the data is a change of the statement, so the tail is redone. Dmut stores a hash of the
data and not the data itself.

## Checks

A check asserts a condition: `check: <sql>`. The query must start with `SELECT` or `WITH` and
return exactly one row with exactly one column of type boolean. `true` passes. `false` fails the
run, and so does any other result, with a message that names the problem. A failed check names the
namespace, the mutation, the position and the first words of the statement.

A check has no down and is skipped when its mutation goes down. It runs each time the tail that
contains it goes up (once, also in the test phase), so a check placed after a dependency is
rechecked whenever that mutation is reapplied. It may carry `settings` and appear in any mutation.

```yaml
deps: [accounts]
sql:
  - check: select count(*) = 0 from accounts where balance < 0
```

---
icon: material/rocket-launch
---

# Getting started

Dmut reads mutations from yaml files, compares them with what a database already has recorded, and
applies the difference — down first, then up — inside a single transaction.

This section covers:

- **[Installation](installation.md)** — `go install` or build from source, and how to run dmut's
  own test suite.
- **[CLI reference](cli-reference.md)** — every subcommand and flag.
- **[How a run works](how-a-run-works.md)** — the exact sequence of steps dmut goes through on
  `apply` and `test`, since it matters for understanding what gets locked and when.

## A first mutation

A minimal mutation file looks like this:

```yaml
# yaml-embedded-languages: sql
# greeting.yml
structural: true
sql:
  - |
    create table greeting (
      id serial primary key,
      message text not null
    )
```

The mutation is named after its file: `greeting.yml` defines the mutation `greeting`. It is
`structural` because it creates a table, which holds data. Save it and apply it:

```sh
dmut apply postgres://user:pass@localhost/mydb ./greeting.yml
```

Dmut creates its own bookkeeping schema (`__dmut__`) on first run, runs the statement, records the
mutation as applied, and — since something ran — tests it: it takes the mutation down and back up
behind a savepoint to make sure it is actually reversible, before committing.

Now add a column. You do not edit the `create table`: you add a statement after it.

```yaml
structural: true
sql:
  - |
    create table greeting (
      id serial primary key,
      message text not null
    )
  - alter table greeting add column lang text not null default 'en'
```

Apply again: dmut sees that the first statement is unchanged and that one was added at the end, so it
runs only the new statement. Nothing comes down, and the table keeps its rows.

If you change a statement that is already there (the `create table` itself), dmut takes down that
statement and everything after it, then brings the new ones up. Here that means `DROP TABLE greeting`
— inferred automatically, see [Automatic down statements](../mutation-format/auto-down.md) — and
**the table and its data are gone**. That is fine in development; see
[Revisions](../revisions/index.md) for how to fold changes back into a clean definition in
production without losing data, and the `-s` flag in the [CLI reference](cli-reference.md) to make an
automated pipeline refuse anything but appends.

For something closer to a real schema — tables that reference each other, functions, generated
columns, grants — see the [example database](../example-database/index.md), which is a full
hotel-booking schema written this way and checked into
[`examples/hotel`](https://github.com/ceymard/dmut/tree/main/examples/hotel).

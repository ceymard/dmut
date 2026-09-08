---
icon: material/rocket-launch
---

# Getting started

Dmut reads mutations from yaml files, computes what changed against what a database already has
recorded, and applies the difference — down first, then up — inside a single transaction.

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
greeting:
  sql:
    - |
      create table greeting (
        id serial primary key,
        message text not null
      );
```

Save it as `greeting.yml` and apply it:

```sh
dmut apply postgres://user:pass@localhost/mydb ./greeting.yml
```

Dmut creates its own bookkeeping schema (`__dmut__`) on first run, records that the `greeting`
mutation is now applied, and — since something changed — replays it down-then-up behind a
savepoint to make sure it's actually reversible, before committing.

Change the file (add a column, drop it, rename something) and apply again: dmut sees the hash of
`greeting`'s `sql` changed, downs it (`DROP TABLE greeting`, inferred automatically — see
[Automatic down statements](../mutation-format/auto-down.md)) and re-creates it from the new
definition. **This drops the table and any data in it.** That's fine in development; see
[Revisions](../revisions/index.md) for how to change a schema in production without losing data.

For something closer to a real schema — tables that reference each other, functions, generated
columns, grants — see the [example database](../example-database/index.md), which is a full
hotel-booking schema written this way and checked into
[`examples/hotel`](https://github.com/ceymard/dmut/tree/main/examples/hotel).

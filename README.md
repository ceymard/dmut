# Dmut, a tool for postgres schema migrations

Dmut is a database migration tool that takes an approach based on dependencies rather than sequential changes.

Whenever a mutation changes, its dependents are recursively undone first before undoing it, then it is redone and its dependents are re-run as well.

When running, dmut performs the following operations :

- Fetch currently applied mutations from the database and compute which need to be de-applied.
- If mutations or meta/roles changed: undo all meta blocks and sync roles (remove obsolete, add missing).
- Optionally run a test in a temporary database (when any change occurred): exercise all mutation down/up combinations, or only meta if sql was unchanged.
- If mutations changed: de-apply and re-apply according to the new `needs` clauses.
- Re-apply meta statements.

As everything is ran inside a transaction, failure at any given step halts the process and mutations are not applied.

# Permissions considerations

# Mutation structure

Mutations are defined in yaml files that are read recursively from the directories dmut is instructed to look at.

Yaml files starting with an `_` will be ignored.

```yaml
name: the mutation name
needs: [optional, parent, mutation, names] # optional
roles: [a, list, of, roles] # optional
sql: a single sql statement or :
  - automatic sql statements or
  - up: the sql that brings this mutation up
    down: the sql that undoes it
meta: just like sql, but with lightweight statements
```

Dmut supports and encourages defining several mutations in the same file with yaml's multi-document `---` separator. In fact, the recommended method of distributing mutations in your docker containers is by `dmut collect`ing them all in a single yaml file.

## Why the distinction between sql and meta

`sql` blocks contain the physical description of your data ; "_what_" will be accessed and modified, whereas `meta` blocks describes the "_how_" (and _by whom_) it is accessed. Changes in the `sql` block can be heavy and lead to loss of data or long processing times. `meta` changes are mostly code and will thus be pretty fast.

Meta could be separate mutations (as in earlier dmut versions), but that approach gets messy: dependency graphs and naming conventions are hard to agree on. Keeping meta next to the objects it manages is simpler, and all `sql` runs before any meta, so objects and roles are guaranteed to exist when meta references them. The split also encourages thinking in terms of heavy (sql) vs light (meta) changes.

## Roles

Roles are collected from _all_ mutations.

## Meta

Permissions, row-level security statements, grants, policies, even triggers and their related functions can be defined in perms blocks.

It is also a fairly safe space to define column expressions, views, and generally any thing that is purely run-time and does not affect the data structure.

As a rule of thumb, try to keep everything related to role to the meta section.

## Changes

A mutation is considered to be different when content differs in `sql` (more than just whitespace or comments), or `name`.

`depends` does not change the mutation, for those cases where you forgot a dependency and for some reason dmut's test did not detect it to be a problem because the order of mutations was fine for a while and don't want to be stuck with something that doesn't work.

`roles` and `perms` are also checked for changes, but do not trigger de-apply or re-apply mechanics, as they have their own life-cycle.

## Naming rules

Dmut understands `.` separators in the mutation names. Mutations that have composite paths like `parent1.parent2.child` automatically depend on mutations named `parent1` and `parent1.parent2` if they exist. They will **not**, however, depend on `parent1.unrelated`.

## Automatic sql statements

For some common SQL statements, undo can be inferred, so you need not specify `up` or `down`. The inferred undo is always a destructive operation (e.g. `DROP`); dmut does not try to restore the database to its exact previous state.

It is recommended to use these statements in `sql` blocks :

- `CREATE TABLE ...`
- `CREATE INDEX ...`
- `CREATE SCHEMA ...`
- `CREATE EXTENSION ...`
- `ALTER TABLE <table> ADD CONSTRAINT <name> <def> ...`
- `ALTER TABLE <table> ADD COLUMN <name> <def> ...` -- (there is no DROP, you must do these with `up` and `down`)

And these in `meta` blocks, as they are not so much about data than behaviour :

- `CREATE FUNCTION <name>`
- `CREATE [MATERIALIZED] VIEW <name>`
- `ALTER TABLE <table> ALTER COLUMN <name> SET DEFAULT`
- `ALTER TABLE <table> ENABLE ROW LEVEL SECURITY`
- `CREATE POLICY <name> ...`
- `CREATE TRIGGER <name> ...`
- `GRANT ...`

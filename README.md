# Dmut, a tool for postgres schema migrations

Dmut is a database migration tool that takes an approach based on dependencies rather than sequential changes.

Whenever a mutation changes, its dependents are recursively undone first before undoing it, then it is redone and its dependents are re-run as well.

When running, dmut performs the following operations :

- Fetch currently applied mutations from the database and compute which need to be de-applied
- If roles changed: undo all meta blocks and sync roles (remove obsolete, add missing).
- In a temporary test database (on the same server), try to run all mutations indepentently.
- If mutations changed: de-apply and re-apply according to the new `needs` clauses. This is where the mutations really are applied.
- Try to down all mutations one by one (this is done using savepoints and does not lose data). The operation is aborted if one of them fails.

As everything is ran inside a transaction, failure at any given step halts the process and mutations are not applied.

# Considerations

- Do not use create "if not exists" or drop "if exists".
- Put data-altering statements whose down incurs loss of data in `sql` blocks:
  - CREATE TABLE
  - CREATE INDEX (data is not lost, but indexes can be slow to create)
  - CREATE TYPE
  - `CREATE SCHEMA ...`
  - `CREATE EXTENSION ...`
  - `ALTER TABLE <table> ADD CONSTRAINT <name> <def> ...`
  - `ALTER TABLE <table> ADD COLUMN <name> <def> ...`

  - ...

- Only put "lightweight" statements in meta blocks : grants, functions, policies and the like.
  - GRANT ...
  - CREATE POLICY
  - CREATE FUNCTION
  - ALTER TABLE ... ENABLE ROW LEVEL SECURITY
  - `CREATE FUNCTION <name>`
  - `CREATE [MATERIALIZED] VIEW <name>`
  - `ALTER TABLE <table> ALTER COLUMN <name> SET DEFAULT`
  - `ALTER TABLE <table> ENABLE ROW LEVEL SECURITY`
  - `CREATE POLICY <name> ...`
  - `CREATE TRIGGER <name> ...`
  - `GRANT ...`

# Handling table modifications during their lifetime

Over the lifetime of your database, your data model will change. Since you do not want to lose already existing data, you will make incremental changes in `children` mutations, for instance to add columns non destructively.

After a while however, having definitions scattered across several mutations becomes untidy. When mutating an empty database, why have a table created and then immediately altered to add or remove their columns ?

You have two options:

- Either change your mutations so that the CREATE TABLE ... has all your columns, backup your data, reapply the mutations and then

# Mutation structure

Mutations are defined in yaml files that are read recursively from the directories dmut is instructed to look at.

Yaml files starting with an `_` will be ignored.

```yaml
mutation_name:
  # optional, names the mutations whose `sql` must run before this mutation
  needs: [optional, parent, mutation, names]

  # optional, list of roles that should exist
  # multiple mutations can declare roles
  roles: [a, list, of, roles]

  # optional, mutations that directly related to this mutation
  children: # optional
    child_name: will be renamed as `mutation_name.child_name`
      sql: # A list of statements, usually alter table ... add ...

  sql:
    - automatic sql statements or
    - up: the sql that brings this mutation up
      down: the sql that undoes it

  # optional, names of the mutations whose meta must run before
  # there is no need to indicate mutations whose sql must run before, because _all_ sql runs before meta, always.
  meta_needs: [mutation, names]

  # Statements of the meta mutation
  meta: just like sql, but with lightweight statements
```

## Why the distinction between sql and meta

`sql` blocks contain the physical description of your data ; "_what_" will be accessed and modified, whereas `meta` blocks describes the "_how_" (and _by whom_) it is accessed. Changes in the `sql` block can be heavy and lead to loss of data or long processing times. `meta` changes are mostly code and will thus be pretty fast.

Meta could be separate mutations (as in earlier dmut versions), but that approach gets messy: dependency graphs and naming conventions are hard to agree on. Keeping meta next to the objects it manages is simpler, and all `sql` runs before any meta, so objects and roles are guaranteed to exist when meta references them. The split also encourages thinking in terms of heavy (sql) vs light (meta) changes.

## Changes

A mutation is considered to be different when content differs in `sql` (more than just whitespace or comments), or `name`.

When a mutation changes, its children and itself will be downed before being re-applied. _BEWARE_ loss of data can happen then, as `CREATE TABLE` mutations that change get `DROP`ped. This is mostly useful in dev where you can change whatever you want and don't mind destoying stuff.

## Naming rules

Dmut understands `.` separators in the mutation names. Mutations that have composite paths like `parent1.parent2.child` automatically depend on mutations named `parent1` and `parent1.parent2` if they exist. They will **not**, however, depend on `parent1.unrelated`.

## Automatic sql statements

For some common SQL statements, undo can be inferred, so you need not specify `up` or `down`. The inferred undo is always a destructive operation (e.g. `DROP`); dmut does not try to restore the database to its exact previous state.

It is recommended to use these statements in `sql` blocks :

- `CREATE TABLE ...`
- `CREATE INDEX ...`

And these in `meta` blocks, as they are not so much about data than behaviour :

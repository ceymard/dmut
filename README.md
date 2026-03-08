# Dmut, a tool for database schema migrations

Dmut is a database migration tool that takes an approach based on dependencies rather than sequential changes.

As of now, dmut only handles postgres, but other databases may be supported if the demand exists.

It features the following :

- Testing :A fairly comprehensive testing system to ensure the mutations you write are reproducible and you don't end up in an unworkable state
- Automatic reverse statements : most of the time, only specify the `create` statements, dmut will
- Revisions :
- Namespaces : to have different sets of mutations on a same database so that team can function independently
- A distinction between "heavy" and "lightweight" statements

Whenever a mutation changes, its dependents are recursively undone first before undoing it, then it is redone and its dependents are re-run as well.

When running, dmut performs the following operations :

- Fetch currently applied mutations from the database and compute which need to be de-applied
- If roles or sql changed (some sql statements have to be downed): undo all meta blocks and sync roles (remove obsolete, add missing).
- If possible, in a temporary test database (on the same server), try to run all mutations indepentently.
- If mutations changed: de-apply and re-apply according to the new `needs` clauses. This is where the mutations really are applied.
- Try to down all mutations one by one (this is done using savepoints and does not lose data). The operation is aborted if one of them fails.

As everything is ran inside a transaction, failure at any given step halts the process and mutations are not applied.

# Considerations

- Do not use create "if not exists" or drop "if exists".
- Never put CASCADE in DROP statement in your custom mutations : dmut relies on every object being created in their mutations and declaring dependencies explicitely. For your safety, it must break during its tests phases if they were not.
- Put data-altering statements whose down incurs loss of data in `sql` blocks:
  - CREATE TABLE
  - CREATE INDEX (data is not lost, but indexes can be slow to create)
  - CREATE TYPE
  - `CREATE SCHEMA ...`
  - `CREATE EXTENSION ...`
  - `ALTER TABLE <table> ADD CONSTRAINT <name> <def> ...`
  - `ALTER TABLE <table> ADD COLUMN <name> <def> ...`

  - ...

- Only put "lightweight" statements in meta blocks : grants, functions, policies and the like. These will be de-applied and re-applied often
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

# Mutation structure

Mutations are defined in yaml files that are read recursively from the directories dmut is instructed to look at.

Yaml files starting with an `_` will be ignored.

```yaml
# optional, indicate that all mutations in this file are part of a revision
__revision: 1
# optional, make all mutations in this file part of a namespace
__namespace: some-name

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

  # optional: when using revisions
  new_needs: [new, parents]

  # optional: when using revisions
  new_sql:
    - statement that replaces what is in `sql`

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

A mutation is considered to be different when content differs in `sql` or `meta`, or `name`. Comments are ignored when computing the hash of a mutation, so that comment changes don't trigger de-apply.

When a mutation changes, its children and itself will be downed before being re-applied. _BEWARE_: loss of data can happen then, as `CREATE TABLE` mutations that change get `DROP`ped. This is mostly useful in dev where you can change whatever you want and don't mind destoying stuff.

## Naming rules

Dmut understands `.` separators in the mutation names. Mutations that have composite paths like `parent1.parent2.child` automatically depend on mutations named `parent1` and `parent1.parent2` if they exist. They will **not**, however, depend on `parent1.unrelated`.

## Automatic sql statements

For some common SQL statements, undo can be inferred, so you need not specify `up` or `down`. The inferred undo is always a destructive operation (e.g. `DROP`); when creating auto-down statement, dmut does **not** query the database to guess how it was before, which is what a lot of `ALTER` statements are not supported in this manner.

It is recommended to use these statements in `sql` blocks :

- `CREATE TABLE ...`
- `CREATE INDEX ...`

And these in `meta` blocks, as they are not so much about data than behaviour :

# Namespaces

Mutations can be namespaced by setting `__namespace: <string>` at the toplevel of their file.

They act as silos ; namespaced mutations will not touch mutations from other namespaces. They may be applied completely independently.

Make absolutely sure that no code from a namespace can reference objects that are created in another ; they are explicitely made to handle completely independent code and structures that will have to live in the same database but will most likely never interact together.

# Revisions : Evolving your mutations over time

As your database evolves, the data model changes. To avoid losing existing data, you add incremental changes in *child* mutations instead of editing existing SQL mutations—so those mutations are not de-applied.

Over time, definitions spread across many mutations become hard to follow. On an empty database, it is redundant to create a table and then immediately alter it to add or remove columns.

You can consolidate by folding those changes back into the original table definition in two ways:

- **Manual rewrite:** Update mutations so each `CREATE TABLE ...` includes all columns, then backup data, reapply mutations, and restore data. This is manageable in development but awkward in production when many databases or large datasets are involved.

- **Revisions:** Use `__revision` and, in revisioned files, `new_needs` / `new_sql`. When a revision is applied, the current `needs` and `sql` run as usual, but the values stored in the database are the `new_` ones—so the recorded source of truth becomes your cleaned-up set. New or empty databases are then seeded from that tidy definition.

## Revisions in your mutation files

You can set `__revision: <int>` at the top level of a YAML file. Every mutation in that file then belongs to that revision.

Mutations in a file with an explicit `__revision` can define `new_needs` and `new_sql`. When such a mutation is applied, `needs` and `sql` run as usual, but the values stored in the database are the `new_` versions (so the “cleaned” definition is what future revisions see).

Files that do not set `__revision` are treated as revision `n+1`, where `n` is the highest revision number found. If no file sets a revision, the effective revision is `1`.

When you supply revisions, dmut applies every revision greater than _or equal_ to the database’s current revision, in order. If the database has no revision, only the highest one is applied.

Revisions **must** be sequential ; there may be no gaps between the lowest and the highest that is supplied.

You do not need to keep every revision file in the codebase. In practice, keep at least the latest revision—or a minimal file that only sets `__revision` so new databases get the right revision number-or the lowest revision that you know is still in production.

To retire an obsolete mutation inside a revision, set `new_sql: []`. Any mutation that has no meta, sql, and children is not persisted.

You should use clear names for revision files, e.g. `<namespace>-r<revision>.yml` or `r<revision>.yml`.

## Creating a new revision

To make revision creation easier, dmut ships the command `dmut create-revision [-o <new-revision-file.yml>] <database or previous-revision.yml> <future revision paths...>` that compares a revision file or the revision currently applied in the database to the local mutations at `paths...` and detects the changes between `sql` and `needs` blocks to create a _new_ revision with the `new_needs` and `new_sql` blocks created automatically.

Use `dmut diff` to compare two revisions and only display what changed.

## Considerations when writing revisions

Mutations that use "complicated" statements like ALTER that cannot be auto-downed are tricky, and dmut makes no attempt at comparing database states : it applies, or it downs. While it does run tests everytime to catch most common errors, it cannot catch them all. The responsability falls on the developer to make sure that the revisions they write make sense.

It is possible to use `meta` blocks to write unit tests in `do $$ begin ... end $$ language plpgsql` statements as usage of the `raise` statement will fail a mutation.
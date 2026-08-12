# Dmut, a tool for database schema migrations

Dmut is a database migration tool that takes an approach based on dependencies rather than
sequential changes. You describe the objects you want and how they depend on each other; dmut
works out what to drop and recreate when a definition changes.

As of now, dmut only handles postgres, but other databases may be supported if the demand exists.

It features the following:

- **Automatic reverse statements**: for most `CREATE`, `ALTER TABLE` and `GRANT` statements you
  only write the "up" and dmut infers the "down".
- **Testing**: on every apply, dmut replays each mutation up-and-down independently to make sure
  your mutations are reproducible and you don't end up in an unworkable state.
- **Revisions**: fold years of incremental `ALTER`s back into clean definitions without losing
  data in the databases already running.
- **Namespaces**: several independent sets of mutations on the same database, so teams can work
  without stepping on each other.
- **A distinction between "heavy" (`sql`) and "lightweight" (`meta`) statements**, so redeploying
  functions, grants and policies never touches your tables.

Whenever a mutation changes, its dependents are recursively undone first, then it is undone,
then it is redone and its dependents are re-run as well.

## Installation

```sh
go install github.com/ceymard/dmut/v2@latest
```

or, from a clone (needs Go 1.25 or later):

```sh
go build -o dmut .
```

## Running the tests

```sh
make test        # everything
make test-unit   # parser, loader, hashing, collect — no database needed
make test-db     # the integration tests only
make test-ci     # everything, and a missing docker daemon fails instead of skipping
```

The integration tests share a single postgres container and give each test a
database of its own. They are skipped when docker is not running, so `go test ./...`
is safe anywhere; set `DMUT_REQUIRE_DOCKER=1` in CI to turn that skip into a
failure. The container is only started if a test actually needs it.

Because postgres roles are cluster-wide rather than per-database, mutations that
create roles cannot be shared between integration tests.

## Usage

```
dmut apply <uri> <paths...>       apply mutations to a database
  -v, --verbose                     echo every statement as it runs
  -d, --dry                         run everything, then roll back instead of committing
  -o, --override                    record the mutations without applying them (see below)

dmut test <paths...>              apply and test against a throwaway postgres (requires Docker)
  -v, --verbose
  -a, --all                         replay every revision from 1, not just the latest
  -i, --test-image <image>          postgres image to run (default: postgres:14)
  -d, --test-database <name>        (default: test)
  -u, --test-username <name>        (default: test)
  -p, --test-password <pass>        (default: test)

dmut down <uri> <namespace>       down everything dmut applied in a namespace
  -v, --verbose
  -d, --dry

dmut collect <outfile> <paths...> write every mutation back out as a single yaml file
dmut explode -o <dir> <paths...>  split a single-document yaml file into one file per mutation
dmut legacy <uri>                 dump a pre-1.0 dmut schema as yaml
dmut version
```

`<uri>` is a libpq/pgx connection string (`postgres://user:pass@host/db`).

`<paths...>` are files or directories. Directories are walked recursively and every `.yml` /
`.yaml` file found is read.

The default namespace is the empty string, so downing unnamespaced mutations reads:

```sh
dmut down postgres://localhost/mydb ""
```

Note that `-d` means `--dry` on `apply` and `down`, but `--test-database` on `test`.

`--override` records the mutations as applied *without running them*. It is how you correct the
record: when the mutations already in the database are wrong, or were never there in the first
place, and you want to hand dmut a corrected version of them without touching the objects
themselves. The test phase still runs, deliberately — the dmut contract is that anything dmut
knows about must be downable, and a new overriding version is no exception. Which means that even
under `--override`, every object in the namespace is downed and re-upped behind savepoints before
the transaction ends. It also means `--override` cannot be the *first* thing dmut does on a
database: it skips creating its own bookkeeping schema along with everything else. Run a plain
apply once before adopting objects with `--override`.

Dmut keeps its own bookkeeping in the `__dmut__` schema (table `__dmut__.mutations`), which it
creates on first run as a namespace of its own.

## How a run works

Everything happens inside a single transaction, per namespace, in ascending revision order.
Failure at any step halts the process and nothing is applied.

- Read the mutations already applied from `__dmut__.mutations` for that namespace.
- Compute the delta. A mutation is obsolete if it no longer exists locally or if its hash
  changed. Obsolete mutations are downed along with everything that depends on them, recursively.
- Down the obsolete `meta`, down the obsolete `sql`, up the new `sql`, then up the new `meta`.
  If *any* `sql` had to be downed, *all* `meta` in the namespace is downed and re-upped, since
  meta may depend on any object the sql touched.
- Record the new state in `__dmut__.mutations`.
- Test, when anything changed: down all `meta`, then replay each mutation's `meta` chain
  up-then-down on its own; then down all `sql` and do the same for `sql`. This runs against the
  target database behind savepoints and is rolled back afterwards, so no data is lost — but every
  mutation must be cleanly downable or the whole apply aborts.
- Commit, or roll back if `-d` was given.

A revision that defines `new_sql` / `new_needs` is additionally tested in its "new" form, so that
the definitions the *next* revision will inherit are known to be valid before anything depends on
them. A database pays for that pass on the run that carries it across that revision, and never
again.

`dmut test` runs the same sequence against a throwaway postgres container and never commits.

## Mutation structure

Mutations are defined in yaml files read recursively from the paths dmut is given.

```yaml
# optional, make all mutations in this file part of a revision
__revision: 1
# optional, make all mutations in this file part of a namespace
__namespace: some-name

mutation_name:
  # optional, names of the mutations whose `sql` must run before this one
  needs: [optional, parent, mutation, names]

  sql:
    - a statement whose down dmut can infer on its own
    - up: the sql that brings this mutation up
      down: the sql that undoes it

  # optional, mutations that belong to this one.
  # `child_name` below is really the mutation `mutation_name.child_name`, and as such
  # it automatically depends on `mutation_name` (see "Naming rules").
  children:
    child_name:
      sql:
        - alter table ... add ...

  # optional, names of the mutations whose `meta` must run before this one.
  # There is no need to name mutations whose *sql* must run before: all `sql` always runs
  # before any `meta`.
  meta_needs: [mutation, names]

  # Lightweight statements, same syntax as `sql`
  meta:
    - grant select on table ... to ...

  # optional, only meaningful in a file that sets `__revision`: replaces `needs` in what gets stored
  new_needs: [new, parents]

  # optional, only meaningful in a file that sets `__revision`: replaces `sql` in what gets stored
  new_sql:
    - statement that replaces what is in `sql`
```

There is no key for roles. Create them in a `sql` block of their own mutation — since all `sql`
runs before any `meta`, they will exist by the time grants and policies reference them:

```yaml
roles:
  sql:
    - create role "@admin";
    - create role "@active";
```

Keys other than the ones above are rejected with an error.

## Why the distinction between sql and meta

`sql` blocks contain the physical description of your data — "*what*" will be accessed and
modified — whereas `meta` blocks describe "*how*" (and *by whom*) it is accessed. Changes in the
`sql` block can be heavy and lead to loss of data or long processing times. `meta` changes are
mostly code and will thus be pretty fast.

The two are hashed separately, so editing a function or a grant never causes a table to be
dropped.

Meta could be separate mutations (as in earlier dmut versions), but that approach gets messy:
dependency graphs and naming conventions are hard to agree on. Keeping meta next to the objects
it manages is simpler, and all `sql` runs before any meta, so objects and roles are guaranteed to
exist when meta references them. The split also encourages thinking in terms of heavy (sql) vs
light (meta) changes.

### What goes in `sql`

Statements whose down would lose data or take a long time:

- `CREATE TABLE ...`
- `CREATE INDEX ...` (no data is lost, but indexes can be slow to create)
- `CREATE TYPE ...`
- `CREATE SCHEMA ...`
- `CREATE EXTENSION ...`
- `CREATE ROLE ...`
- `ALTER TABLE <table> ADD COLUMN ...`
- `ALTER TABLE <table> ADD CONSTRAINT ...`

### What goes in `meta`

Lightweight statements that describe behaviour and can be de-applied and re-applied often:

- `CREATE FUNCTION ...` / `CREATE PROCEDURE ...`
- `CREATE [MATERIALIZED] VIEW ...`
- `CREATE TRIGGER ...`
- `CREATE POLICY ...`
- `ALTER TABLE <table> ENABLE ROW LEVEL SECURITY`
- `ALTER TABLE <table> ALTER COLUMN <name> SET DEFAULT ...`
- `GRANT ...`
- `ALTER DEFAULT PRIVILEGES ...`

## Automatic down statements

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

`mutations/test/auto-down-tests.yml` is the authoritative list: every supported form appears there
with the down dmut generates for it.

If dmut cannot infer a down, loading the file fails with an error — write the `up`/`down` pair
yourself. In particular:

- A bare `DROP ...` never auto-downs. Dmut only understands `CREATE`, `ALTER TABLE`, `GRANT`,
  `ALTER DEFAULT PRIVILEGES` and `COMMENT ON`.
- `COMMENT ON` has no down at all — dmut leaves the comment in place rather than guess what it used
  to be. It is still written as a plain statement; "no down" and "we could not find a down" are
  different outcomes, and only the second one is an error.

## Collecting

`dmut collect <outfile> <paths...>` reads everything dmut would read and writes it back out as a
single yaml file — one document per namespace and revision, mutations sorted by name. Pass `-` as
the outfile to write to stdout.

```sh
dmut collect schema.yml ./mutations
```

The output is meant to be read back by dmut, and is normalised on the way out: statements whose
down can be inferred are written as plain strings, the rest as `up`/`down` pairs, `children:` are
flattened to their real `parent.child` names, and the dependencies dmut derives from dotted names
are left out since it derives them again on load. Collecting an already collected file changes
nothing.

Collecting never changes a mutation's hash, so pointing dmut at a collected file instead of your
tree applies as a no-op to an existing database.

Dmut's own `__dmut__` mutations are left out — they ship inside the binary.

`dmut explode` is not the inverse of `collect`. It is there to lay the *current* revision out as
neat per-mutation files — the version of the code you are actually working on — and it should be
run on that revision alone. It splits with a regex and knows nothing about documents, so given
several revisions at once, mutations that share a name across them overwrite each other silently.

`__namespace` is written at the top of every file it produces, since each of them is read back as a
set of its own. `__revision` is deliberately not: a file that sets no revision is the current one by
definition, which is exactly what exploded code is. A file declaring more than one namespace is
refused rather than split across silos.

## Naming rules

Dmut understands `.` separators in mutation names. A mutation named `parent1.parent2.child`
automatically depends on `parent1` and `parent1.parent2` if they exist, for both `sql` and `meta`.
It will **not** depend on `parent1.unrelated`.

Mutations declared under `children:` are named `<parent>.<child>` and therefore pick up that
dependency automatically.

Duplicate mutation names within the same namespace and revision are an error, including across
different files — the error names both files.

Dependency cycles are detected at load time and reported with the offending path.

## Changes

A mutation is considered different when its `name` or the content of its `sql` or `meta` changes.
The `down` half of a statement counts too: editing only an explicit `down:` will trigger a
de-apply.

Comments and whitespace are normalised away before hashing, so reformatting or commenting your SQL
does not cause anything to be re-applied.

When a mutation changes, it and its dependents are downed before being re-applied. *BEWARE*: loss
of data can happen then, as `CREATE TABLE` mutations that change get `DROP`ped. This is mostly
useful in dev where you can change whatever you want and don't mind destroying stuff. In
production, use revisions.

## Namespaces

Mutations can be namespaced by setting `__namespace: <string>` at the toplevel of their file.

They act as silos; namespaced mutations will not touch mutations from other namespaces, and may be
applied completely independently.

`needs` and `meta_needs` cannot cross a namespace — naming a mutation from another namespace is a
load-time error. Dmut cannot see through raw SQL, though, so make absolutely sure that no code from
a namespace references objects created in another. Namespaces are explicitly made for completely
independent code and structures that live in the same database but never interact.

## Revisions: evolving your mutations over time

As your database evolves, the data model changes. To avoid losing existing data, you add
incremental changes in *child* mutations instead of editing existing SQL mutations — so those
mutations are not de-applied.

Over time, definitions spread across many mutations become hard to follow. On an empty database, it
is redundant to create a table and then immediately alter it to add or remove columns.

You can consolidate by folding those changes back into the original table definition in two ways:

- **Manual rewrite**: update mutations so each `CREATE TABLE ...` includes all columns, then back
  up data, reapply mutations, and restore data. This is manageable in development but awkward in
  production when many databases or large datasets are involved.

- **Revisions**: use `__revision` and, in revisioned files, `new_needs` / `new_sql`. When a
  revision is applied, the current `needs` and `sql` run as usual, but the values stored in the
  database are the `new_` ones — so the recorded source of truth becomes your cleaned-up set. New
  or empty databases are then seeded from that tidy definition.

### `new_sql` and `new_needs` are transitory

They are only ever read when a *later* revision is applied on top: dmut reaches for the stored
`new_` values precisely when the database's revision is lower than the one being applied. So they
come in pairs — write `new_sql` in revision `n` only when you are shipping revision `n+1` that
contains the consolidated definitions those `new_` values describe.

A revision carrying `new_sql` with nothing after it is a half-finished job: nothing will ever read
what it stored. Once revision `n+1` exists and your databases have moved past `n`, the `new_`
blocks in `n` have done their work and the file can eventually be dropped.

### Revisions in your mutation files

Set `__revision: <int>` at the top level of a yaml file; every mutation in that file belongs to
that revision.

**Each revision must be self-contained.** A revision restates every mutation it consists of, not
just what changed since the previous one — `needs` cannot reach into another revision any more than
it can reach into another namespace. See `test/revision/test-r1.yml` through `test-r3.yml` for a
worked example.

Files that do not set `__revision` are treated as revision `n+1`, where `n` is the highest revision
number found *in that namespace*. If no file in a namespace sets a revision, its effective revision
is `1`.

Revisions **must** be sequential; there may be no gaps between the lowest and the highest supplied.

When you supply revisions, dmut applies every revision greater than *or equal* to the database's
current revision, in order. If the database has no revision yet, only the highest one is applied —
`dmut test -a` overrides this and replays every revision from 1, which is what you want in CI.

You do not need to keep every revision file in the codebase. In practice, keep at least the latest
revision — or a minimal file that only sets `__revision` so new databases get the right revision
number — or the lowest revision that you know is still in production.

To retire a mutation in a new revision, set `new_sql: []`. It is then recorded with an empty `sql`,
so the following revision downs it as a no-op and it drops out of the graph. A mutation disappears
from `__dmut__.mutations` entirely only when it has no `sql`, `meta`, `needs`, `meta_needs`,
`new_sql` and `new_needs` at all.

Use clear names for revision files, e.g. `<namespace>-r<revision>.yml` or `r<revision>.yml`.

### Considerations when writing revisions

Mutations that use "complicated" statements like `ALTER`, which cannot be auto-downed, are tricky,
and dmut makes no attempt at comparing database states: it applies, or it downs. While it does run
tests every time to catch the most common errors, it cannot catch them all. The responsibility
falls on the developer to make sure the revisions they write make sense.

You can use `meta` blocks to write unit tests in `do $$ begin ... end $$ language plpgsql`
statements, since a `raise` will fail the mutation. `test/revision/test-r3.yml` uses this to assert
that no data was lost across a revision.

## Considerations

- **Do not use `CREATE ... IF NOT EXISTS` or `DROP ... IF EXISTS`.** Dmut is meant to own the whole
  life cycle of a symbol in the database: if a statement silently succeeds whether or not the
  object is there, a botched down goes unnoticed and the test phase can no longer catch it. Dmut
  will parse and auto-down `CREATE ... IF NOT EXISTS` without complaining — that leniency is there
  for the case where the database already contains a symbol and you are adding dmut on top of it.
  Outside that case, don't. (`DROP ... IF EXISTS` has no auto-down at all; no bare `DROP` does.)

- **Never put `CASCADE` in a `DROP` statement** in your custom mutations. Dmut relies on every
  object being created in its own mutation with dependencies declared explicitly. For your safety,
  it must break during the test phase when they were not.

- **Every mutation must be downable.** The test phase downs and re-ups everything; a mutation that
  cannot be undone will fail the whole apply, not just itself.

- `needs` and `meta_needs` may not cross a namespace or a revision.

- **Write `new_sql` only alongside the next revision.** See
  [`new_sql` and `new_needs` are transitory](#new_sql-and-new_needs-are-transitory).

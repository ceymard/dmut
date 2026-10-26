---
icon: material/swap-horizontal
---

# Migrating from dmut 2

Dmut 3 reads a new file format and keeps its state in new tables. There is no automatic conversion:
mutation files written for dmut 2 have to be rewritten, and a database that dmut 2 applied has to be
reset. This page maps the old concepts to the new ones.

## The database

Dmut 3 refuses a database whose `__dmut__` schema was written by an earlier version, and says so.
The old `__dmut__.mutations` table cannot be read. To move an existing database:

1. Make sure the objects in the database match your new files.
2. Drop the `__dmut__` schema (`drop schema __dmut__ cascade`). This forgets what dmut recorded and
   touches none of your objects.
3. Run `dmut apply -o` on your new files. `--override` records the mutations without running them,
   and the test phase checks that they really match what is in the database, so a file that does not
   describe the database fails there. See [`--override`](getting-started/cli-reference.md#override).

On a throwaway database, drop the schemas and apply normally.

## Files

| Dmut 2 | Dmut 3 |
|---|---|
| `__namespace: x` | `namespace: x` |
| `__revision: 2` | a revision document: `revision: 2` as the first key |
| several mutations in one file, as top-level keys | one file per mutation, named after the mutation, or a revision document with `mutations:` |
| `children: {c: ...}` | a mutation named `parent.c`, in its own file or under `mutations:` |
| `core:` | `sql:` with `structural: true` |
| `glue:` | a separate default mutation (usually a child) holding the glue statements |
| `needs: [a]` and `glue_needs: [b]` | `deps: [a, b]` |
| `new_sql`, `new_needs` | `override:` in a revision document |
| `mode: default`, `simple`, `full` | `mode: normal`, `safe`, `force` |
| `-s`, `--simple` | `-s`, `--safe` |
| `-f`, `--full` | `-f`, `--force` |

### `core` and `glue`

Dmut 2 ran all the `core` of a namespace before any `glue`, and rebuilt all the `glue` whenever some
`core` came down. Dmut 3 has no global order: everything is a mutation, ordered by its dependencies.

- A `core` block becomes the `sql` of a structural mutation.
- A `glue` block (functions, views, triggers, policies, grants, comments) becomes its own default
  mutation, usually named as a child of the table it goes with (`hotel.rooms.functions`). List in
  `deps` what its statements use, since nothing is ordered globally any more.
- To get the old "glue follows the core" behaviour, give the glue mutation
  `reapply_on: [dep_reapplied, dep_appended]`. Statements appended to a table then reapply the
  functions and views that depend on it, and a grants mutation can depend on the whole schema with a
  pattern (`hotel.**`).
- Seed data and `do $$ ... $$` checks that sat in `core` become `seed:` and `ensure:` statements. A
  seed must be safe to run twice: the test phase runs it twice.
- A statement string that held several SQL statements must be split, or written as `up`/`down`.
  `ALTER TYPE ... ADD VALUE` needs `IF NOT EXISTS`, an explicit `down`, or `seed:`.
- A statement outside the [default list](mutation-format/structural.md#what-a-default-mutation-may-hold)
  (a `DROP TABLE`, an `ALTER TABLE ... ADD COLUMN`) is refused in a default mutation: mark the
  mutation `structural: true`.

### Appends

In dmut 2, editing a mutation's `core` brought its tail down and up; appending a statement ran only
the new one. That has not changed, and now a new column no longer needs a child mutation: it can be one
more statement at the end of the table's `sql`.

## Behaviour that changed

- A structural mutation is never reapplied because a dependency changed, unless it sets
  `reapply_on`. In dmut 2 a blocked `core` tail failed the run unless `-f` was given; it still does,
  and `-f` still reapplies the dependents.
- The test phase no longer simulates every single-statement edit. It reapplies each mutation with
  its dependents, then takes everything down and up with seeds run twice.
- Log records no longer have a `kind` field. They carry `reason` and `cause` when a reapply or a
  setting caused them.
- `dmut explode` writes the effective definition from the loaded files, one `<mutation>.yml` each,
  not a regex split of the text.

## Dmut 1 files

`dmut legacy <uri>` still reads the schema of a dmut older than 1.0.0 and writes it as a revision
document in the current format, with every mutation `structural: true`. A statement without a
matching up or down is written with only the half it has, so that loading the file stops there and
you write the missing half. Review the result, narrow the mutations that can be default ones, and
follow the steps above.

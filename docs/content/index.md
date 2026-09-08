---
icon: material/database-sync
---

# Dmut

Dmut is a database migration tool that takes an approach based on dependencies rather than
sequential changes. You describe the objects you want and how they depend on each other; dmut
works out what to drop and recreate when a definition changes.

As of now, dmut only handles Postgres, but other databases may be supported if the demand exists.

It features the following:

- **Automatic reverse statements** — for most `CREATE`, `ALTER TABLE` and `GRANT` statements you
  only write the "up" and dmut infers the "down". See
  [Automatic down statements](mutation-format/auto-down.md).
- **Testing** — on every apply, dmut replays each mutation up-and-down independently to make sure
  your mutations are reproducible and you don't end up in an unworkable state.
- **Revisions** — fold years of incremental `ALTER`s back into clean definitions without losing
  data in the databases already running. See [Revisions](revisions/index.md).
- **Namespaces** — several independent sets of mutations on the same database, so teams can work
  without stepping on each other. See [Namespaces](mutation-format/namespaces.md).
- **A distinction between "heavy" (`sql`) and "lightweight" (`meta`) statements**, so redeploying
  functions, grants and policies never touches your tables. See
  [sql vs meta](mutation-format/sql-vs-meta.md).

Whenever a mutation changes, its dependents are recursively undone first, then it is undone, then
it is redone and its dependents are re-run as well.

## Scope

Dmut is built for development ease — fast, safe-to-experiment iteration on a schema, and
reproducible deploys — not for zero-downtime changes on a large, continuously-hammered production
database. Every apply that has changes runs its test phase against the *target* database: every
object in the namespace gets downed and re-upped, individually, behind savepoints, inside the same
transaction as the apply. That means real locks on real objects for the duration, scaling with how
much is in the namespace. There is no flag to skip this on `apply`. If your database can't tolerate
that, run `dmut test` against a throwaway copy to validate the mutations, review the change, and
accept that dmut's production `apply` is not built to route around a database that must stay fully
available while it runs.

## In this documentation

- **[Getting started](getting-started/index.md)** — install dmut, write your first mutation, and
  understand what actually happens on `apply`.
- **[Mutation format](mutation-format/index.md)** — the full yaml structure: `sql`/`meta`,
  `needs`/`meta_needs`, `children`, auto-down, namespaces.
- **[Revisions](revisions/index.md)** — how to consolidate years of incremental changes without
  losing data, walked through with a real worked example.
- **[Example database](example-database/index.md)** — a hotel-booking schema, deep enough in
  relationships and Postgres types to be a realistic tour of everything above, written entirely in
  dmut mutations. It ships in this repository as [`examples/hotel`](
  https://github.com/ceymard/dmut/tree/main/examples/hotel), and every mutation in it is verified
  by `dmut test` in CI.
- **[Considerations](considerations.md)** — the rules that keep dmut's guarantees intact
  (`IF NOT EXISTS`, `CASCADE`, downability) and why they exist.

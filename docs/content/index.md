---
icon: material/database-sync
---

# Dmut

Dmut is a database migration tool that takes an approach based on dependencies rather than
sequential changes. You describe the objects you want and how they depend on each other; dmut
works out what to take down and bring back up when a definition changes.

As of now, dmut only handles Postgres, but other databases may be supported if the demand exists.

It features the following:

- **Automatic reverse statements** — for most `CREATE`, `ALTER TABLE` and `GRANT` statements you
  only write the "up" and dmut infers the "down". See
  [Automatic down statements](mutation-format/auto-down.md).
- **Testing** — on every apply that runs something, dmut reapplies each mutation with its
  dependents, then takes everything down and up again, to make sure your mutations are
  reproducible and you don't end up in an unworkable state.
- **Appends that cost nothing** — adding statements at the end of a mutation runs only the new
  statements, so a new column is one more line, not a new mutation and not a dropped table.
- **Structural and default mutations** — mutations that hold data are declared as such and are
  never reapplied unless they ask; functions, views and grants are reapplied automatically when
  what they depend on changes. See [Structural and default mutations](mutation-format/structural.md).
- **Dependencies and reapply triggers** — name what a mutation depends on, with patterns, and say
  which events make it reapply. See [Dependencies and reapply triggers](mutation-format/dependencies.md).
- **Seeds and checks** — statements with no way back for reference data, and assertions that fail
  the run. See [Statements, seeds and checks](mutation-format/statements.md).
- **Settings** — hand environment variables and values to statements. See
  [Settings](mutation-format/settings.md).
- **Revisions** — fold years of incremental `ALTER`s back into clean definitions without losing
  data in the databases already running. See [Revisions](revisions/index.md).
- **Namespaces** — several independent sets of mutations on the same database, so teams can work
  without stepping on each other. See [Namespaces](mutation-format/namespaces.md).

Whenever the statements of a mutation change, the part that changed comes down and goes back up.
Whatever depends on a mutation that is reapplied can follow it, depending on the triggers it asked
for.

## Scope

Dmut is built for development ease — fast, safe-to-experiment iteration on a schema, and
reproducible deploys — not for zero-downtime changes on a large, continuously-hammered production
database. Every apply that runs something also runs its test phase against the *target* database:
every mutation in the namespace gets reapplied with its dependents, and then everything is taken
down and up again, behind savepoints, inside the same transaction as the apply. That means real
locks on real objects for the duration, scaling with how much is in the namespace. There is no
flag to skip this on `apply`. If your database can't tolerate that, run `dmut test` against a
throwaway copy to validate the mutations, review the change, and accept that dmut's production
`apply` is not built to route around a database that must stay fully available while it runs.

## In this documentation

- **[Getting started](getting-started/index.md)** — install dmut, write your first mutation, and
  understand what actually happens on `apply`.
- **[Mutation format](mutation-format/index.md)** — the full yaml structure: files, statements,
  structural mutations, dependencies, settings, auto-down, namespaces.
- **[Revisions](revisions/index.md)** — how to consolidate years of incremental changes without
  losing data, walked through with a real worked example.
- **[Example database](example-database/index.md)** — a hotel-booking schema, deep enough in
  relationships and Postgres types to be a realistic tour of everything above, written entirely in
  dmut mutations. It ships in this repository as [`examples/hotel`](
  https://github.com/ceymard/dmut/tree/main/examples/hotel), and every mutation in it is verified
  by `dmut test` in CI.
- **[Migrating from dmut 2](migrating.md)** — how the old `core`/`glue` files map to the new
  format.
- **[Considerations](considerations.md)** — the rules that keep dmut's guarantees intact
  (`IF NOT EXISTS`, `CASCADE`, downability) and why they exist.

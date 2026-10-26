# Dmut, a tool for database schema migrations

Dmut is a database migration tool that takes an approach based on dependencies rather than
sequential changes. You describe the objects you want and how they depend on each other; dmut
works out what to take down and bring back up when a definition changes.

As of now, dmut only handles postgres, but other databases may be supported if the demand exists.

It features the following:

- **Automatic reverse statements**: for most `CREATE`, `ALTER TABLE` and `GRANT` statements you
  only write the "up" and dmut infers the "down".
- **Testing**: on every apply that runs something, dmut reapplies each mutation with its dependents,
  then takes everything down and up again, to make sure your mutations are reproducible and you
  don't end up in an unworkable state.
- **Appends that cost nothing**: adding statements at the end of a mutation runs only the new
  statements.
- **Structural and default mutations**: what holds data is never reapplied unless it asks; functions,
  views and grants follow the things they depend on.
- **Dependencies, patterns and reapply triggers**, **seeds** and **checks**, and **settings** handed
  to statements from the environment.
- **Revisions**: fold years of incremental `ALTER`s back into clean definitions without losing
  data in the databases already running.
- **Namespaces**: several independent sets of mutations on the same database, so teams can work
  without stepping on each other.

## A first mutation

```yaml
# greeting.yml - the mutation is named after its file
structural: true
sql:
  - |
    create table greeting (
      id serial primary key,
      message text not null
    )
  # a new column is one more statement at the end
  - alter table greeting add column lang text not null default 'en'
```

```sh
dmut apply postgres://user:pass@localhost/mydb ./greeting.yml
```

Dmut compares each mutation with what it recorded, statement by statement. An added statement runs
alone. A changed statement takes it and everything after it down, then up again — which drops a table
if the `create table` itself changed. `dmut apply -s` refuses anything but appends, which is what an
automated pipeline wants.

## Scope

Dmut is built for development ease — fast, safe-to-experiment iteration on a schema, and
reproducible deploys — not for zero-downtime changes on a large, continuously-hammered
production database. Every apply that runs something also runs its test phase against the *target*
database: every mutation gets reapplied with its dependents, and then everything is taken down and up
again, behind savepoints, inside the same transaction as the apply. That means real locks on real
objects for the duration, scaling with how much is in the namespace. There is no flag to skip this on
`apply`. If your database can't tolerate that, run `dmut test` against a throwaway copy to validate
the mutations, review the change, and accept that dmut's production `apply` is not built to route
around a database that must stay fully available while it runs.

## Installation

```sh
go install github.com/ceymard/dmut/v3@latest
```

or, from a clone (needs Go 1.25 or later):

```sh
go build -o dmut .
```

## Commands

```
dmut apply <uri> <paths...>   apply mutations to a database (-d dry run, -s safe, -f force, -o override, -a all, --set N=value)
dmut test <paths...>          apply and test against a throwaway postgres (needs Docker)
dmut down <uri> <namespace>   down everything dmut applied in a namespace
dmut collect <out> <paths...> write every mutation back out as a single yaml file (-r for a revision shell)
dmut explode --out-dir <dir> <paths...>   write one file per mutation
```

## Documentation

The documentation lives in `docs/content/` and is built with [Zensical](https://zensical.org/)
(`just docs-serve`, `just docs-build`). It covers the mutation format, dependencies and reapply
triggers, seeds, checks and settings, revisions, the CLI, how a run works, a walked-through example
database ([`examples/hotel`](examples/hotel)), two revisions examples
([`examples/revisions`](examples/revisions) and [`examples/revisions-tutorial`](
examples/revisions-tutorial)), and how to [migrate from dmut 2](docs/content/migrating.md).

## Running the tests

```sh
just test        # everything
just test-unit   # loader, planner, parser — no database needed
just test-db     # the integration tests only
just test-ci     # everything, and a missing docker daemon fails instead of skipping
```

The integration tests share a single postgres container and give each test a database of its own.
They are skipped when docker is not running, so `go test ./...` is safe anywhere; set
`DMUT_REQUIRE_DOCKER=1` in CI to turn that skip into a failure, and `DMUT_TEST_IMAGE` to pick the
postgres image (`postgres:14` by default). The container is only started if a test actually needs it.

Because postgres roles are cluster-wide rather than per-database, mutations that create roles cannot
be shared between integration tests.

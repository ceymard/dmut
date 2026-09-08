# Installation

```sh
go install github.com/ceymard/dmut/v2@latest
```

or, from a clone (needs Go 1.25 or later):

```sh
go build -o dmut .
```

## Running the tests

```sh
just test        # everything
just test-unit   # parser, loader, hashing, collect — no database needed
just test-db     # the integration tests only
just test-ci     # everything, and a missing docker daemon fails instead of skipping
```

The integration tests share a single Postgres container and give each test a database of its own.
They are skipped when docker is not running, so `go test ./...` is safe anywhere; set
`DMUT_REQUIRE_DOCKER=1` in CI to turn that skip into a failure. The container is only started if a
test actually needs it.

Because Postgres roles are cluster-wide rather than per-database, mutations that create roles
cannot be shared between integration tests.

## Building this documentation

The doc site itself uses [Zensical](https://zensical.org/):

```sh
just docs-serve   # live reload at http://127.0.0.1:8000
just docs-build   # static site into docs/site
```

Both create/refresh a `.venv-docs` virtualenv on their own; see the `justfile` if you'd rather run
`uv`/`zensical` directly.

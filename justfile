build:
    go build -ldflags "-X main.VERSION=$(git describe --tags --always --dirty)" -o dmut .

# Everything. The integration tests are skipped if docker is not running.
test:
    go test ./... -count=1

# Only the tests that need no database : parser, loader, hashing, collect.
test-unit:
    go test ./mutations/model/ -count=1

# Only the tests that run against a real postgres.
test-db:
    go test . ./mutations/ -count=1

# Same as test, but a missing docker daemon is a failure rather than a skip.
test-ci:
    DMUT_REQUIRE_DOCKER=1 go test ./... -count=1

# Everything in examples/, replaying every revision from 1 (requires Docker).
test-examples: build
    ./dmut test -a ./examples/hotel
    ./dmut test -a ./examples/revisions

lint:
    go vet ./...

clean:
    rm -f dmut

# --- Documentation (Zensical) ---
# Config, content and examples referenced via snippets all live under ./docs
# and ./examples ; docs/zensical.toml is the project root -f points at.

docs_venv := ".venv-docs"
docs_config := "docs/zensical.toml"

# Create/refresh the docs tooling virtualenv.
docs-install:
    uv venv {{docs_venv}} 2>/dev/null || true
    uv pip install --python {{docs_venv}}/bin/python zensical

# Serve the docs locally with live reload at http://127.0.0.1:8000
docs-serve: docs-install
    {{docs_venv}}/bin/zensical serve -f {{docs_config}}

# Build the static site into docs/site
docs-build: docs-install
    {{docs_venv}}/bin/zensical build -f {{docs_config}}

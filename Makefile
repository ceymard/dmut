.PHONY: build test test-unit test-db test-ci lint clean

build:
	go build -o dmut .

# Everything. The integration tests are skipped if docker is not running.
test:
	go test ./... -count=1

# Only the tests that need no database : parser, loader, hashing, collect.
test-unit:
	go test ./mutations/ -count=1

# Only the tests that run against a real postgres.
test-db:
	go test . -count=1

# Same as test, but a missing docker daemon is a failure rather than a skip.
test-ci:
	DMUT_REQUIRE_DOCKER=1 go test ./... -count=1

lint:
	go vet ./...

clean:
	rm -f dmut

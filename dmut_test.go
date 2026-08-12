package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// The integration tests share one postgres container and give each test its own
// database inside it, which is much faster than a container per test.
//
// The container is started on first use, so tests that need no database cost
// nothing. When docker is unavailable they are skipped, unless DMUT_REQUIRE_DOCKER
// is set - CI should set it so that a missing daemon fails instead of passing
// quietly.

var (
	container_once sync.Once
	container      *postgres.PostgresContainer
	container_uri  string
	container_err  error
	database_count atomic.Int64
)

func startContainer() {
	ctx := context.Background()

	container, container_err = postgres.Run(ctx,
		"postgres:14",
		postgres.WithDatabase("dmut"),
		postgres.WithUsername("dmut"),
		postgres.WithPassword("dmut"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if container_err != nil {
		return
	}

	container_uri, container_err = container.ConnectionString(ctx, "sslmode=disable")
}

// testdb creates a database of its own and returns its uri. Postgres roles are
// cluster-wide, so mutations that create roles cannot be used from more than one
// test - the databases are separate, the cluster is not.
func testdb(t *testing.T) string {
	t.Helper()

	container_once.Do(startContainer)
	if container_err != nil {
		if os.Getenv("DMUT_REQUIRE_DOCKER") != "" {
			t.Fatalf("could not start postgres: %v", container_err)
		}
		t.Skipf("skipping, could not start postgres (set DMUT_REQUIRE_DOCKER to make this a failure): %v", container_err)
	}

	name := fmt.Sprintf("test_%d", database_count.Add(1))

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, container_uri)
	if err != nil {
		t.Fatalf("could not connect to postgres: %v", err)
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("could not create database %s: %v", name, err)
	}

	parsed, err := url.Parse(container_uri)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + name
	return parsed.String()
}

func TestMain(m *testing.M) {
	code := m.Run()

	if container != nil {
		_ = container.Terminate(context.Background())
	}

	os.Exit(code)
}

// //////////////////////////////////////////////////////////////
// Helpers

// mutationDir writes yaml files in a fresh directory and returns its path.
func mutationDir(t *testing.T, files map[string]string) string {
	t.Helper()

	dir := t.TempDir()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// apply runs the apply command the way the cli does.
func apply(t *testing.T, uri string, paths ...string) error {
	t.Helper()
	return ApplyCmd{Uri: uri, Paths: paths}.Run()
}

// mustApply fails the test when the mutations do not apply.
func mustApply(t *testing.T, uri string, paths ...string) {
	t.Helper()

	if err := apply(t, uri, paths...); err != nil {
		t.Fatalf("expected the mutations to apply, got: %v", err)
	}
}

func exec(t *testing.T, uri string, sql string) {
	t.Helper()

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("error running %s: %v", sql, err)
	}
}

// queryInt runs a query returning a single number.
func queryInt(t *testing.T, uri string, sql string) int {
	t.Helper()

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	var res int
	if err := conn.QueryRow(ctx, sql).Scan(&res); err != nil {
		t.Fatalf("error running %s: %v", sql, err)
	}
	return res
}

// exists reports whether a relation, schema or column is there.
func exists(t *testing.T, uri string, sql string) bool {
	t.Helper()
	return queryInt(t, uri, "select count(*) from ("+sql+") _q") > 0
}

func tableExists(t *testing.T, uri string, name string) bool {
	t.Helper()
	return exists(t, uri, "select 1 from pg_tables where tablename = '"+name+"'")
}

func schemaExists(t *testing.T, uri string, name string) bool {
	t.Helper()
	return exists(t, uri, "select 1 from pg_namespace where nspname = '"+name+"'")
}

func columnExists(t *testing.T, uri string, table string, column string) bool {
	t.Helper()
	return exists(t, uri,
		"select 1 from information_schema.columns where table_name = '"+table+"' and column_name = '"+column+"'")
}

// dbRevision is the revision dmut recorded for a namespace, 0 when it knows none.
func dbRevision(t *testing.T, uri string, namespace string) int {
	t.Helper()

	if !schemaExists(t, uri, "__dmut__") {
		return 0
	}
	return queryInt(t, uri,
		"select coalesce(max(revision), 0) from __dmut__.mutations where namespace = '"+namespace+"'")
}

// savedMutations is the number of mutations dmut recorded for a namespace.
func savedMutations(t *testing.T, uri string, namespace string) int {
	t.Helper()

	if !schemaExists(t, uri, "__dmut__") {
		return 0
	}
	return queryInt(t, uri,
		"select count(*) from __dmut__.mutations where namespace = '"+namespace+"'")
}

func requireErrorContaining(t *testing.T, err error, want string) {
	t.Helper()

	if err == nil {
		t.Fatalf("expected an error containing %q, got none", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Errorf("expected an error containing %q, got: %v", want, err)
	}
}

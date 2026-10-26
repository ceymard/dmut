package mutations

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// The tests share one postgres container and give each test a database of its own. The
// container starts on first use; without docker the tests are skipped, unless
// DMUT_REQUIRE_DOCKER is set. DMUT_TEST_IMAGE picks the image (postgres:14 by default).

var (
	containerOnce sync.Once
	container     *postgres.PostgresContainer
	containerURI  string
	containerErr  error
	databaseCount atomic.Int64
)

func startContainer() {
	ctx := context.Background()
	image := os.Getenv("DMUT_TEST_IMAGE")
	if image == "" {
		image = "postgres:14"
	}
	container, containerErr = postgres.Run(ctx, image,
		postgres.WithDatabase("dmut"),
		postgres.WithUsername("dmut"),
		postgres.WithPassword("dmut"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second),
		),
	)
	if containerErr != nil {
		return
	}
	containerURI, containerErr = container.ConnectionString(ctx, "sslmode=disable")
}

// testdb creates a database for one test and returns its uri. Roles are cluster-wide, so
// mutations that create roles must not be used from more than one test.
func testdb(t *testing.T) string {
	t.Helper()
	containerOnce.Do(startContainer)
	if containerErr != nil {
		if os.Getenv("DMUT_REQUIRE_DOCKER") != "" {
			t.Fatalf("could not start postgres: %v", containerErr)
		}
		t.Skipf("skipping, could not start postgres (set DMUT_REQUIRE_DOCKER to make this a failure): %v", containerErr)
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, containerURI)
	if err != nil {
		t.Fatalf("could not connect to postgres: %v", err)
	}
	defer conn.Close(ctx)
	name := fmt.Sprintf("test_%d", databaseCount.Add(1))
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("could not create database %s: %v", name, err)
	}
	parsed, err := url.Parse(containerURI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + name
	return parsed.String()
}

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	code := m.Run()
	if container != nil {
		_ = container.Terminate(context.Background())
	}
	os.Exit(code)
}

// runner connects to a fresh database.
func runner(t *testing.T) *Runner {
	t.Helper()
	r, err := NewRunner(context.Background(), testdb(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close(context.Background()) })
	return r
}

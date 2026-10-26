package main

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/ceymard/dmut/v2/mutations"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type TestCmd struct {
	All      bool     `short:"a" help:"Test all revisions, not just the latest one."`
	Image    string   `name:"test-image" help:"Postgres image name to test on."`
	Database string   `name:"test-database" help:"Database name to test on."`
	Username string   `name:"test-username" help:"Username to test on."`
	Password string   `name:"test-password" help:"Password to test on."`
	Paths    []string `arg:"" help:"Paths to test."`
}

type Printer struct{}

func (Printer) Accept(l testcontainers.Log) {
	slog.Debug("postgres", "log", strings.TrimRight(string(l.Content), "\n"))
}

func (t TestCmd) Run(ctx context.Context) error {
	var image = "postgres:14"
	if t.Image != "" {
		image = t.Image
	}
	if t.Database == "" {
		t.Database = "test"
	}
	if t.Username == "" {
		t.Username = "test"
	}
	if t.Password == "" {
		t.Password = "test"
	}

	project, err := mutations.LoadProject(t.Paths...)
	if err != nil {
		return err
	}

	slog.Info("testing mutations", "image", image)
	container, err := postgres.Run(ctx,
		image,
		postgres.WithDatabase(t.Database),
		postgres.WithUsername(t.Username),
		postgres.WithPassword(t.Password),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(5*time.Second),
		),
	)
	if err != nil {
		return err
	}
	// The container must go away even when ctx was cancelled.
	defer func() {
		cleanup_ctx, cancel := mutations.CleanupContext(ctx)
		defer cancel()
		container.Terminate(cleanup_ctx)
	}()

	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		printer := Printer{}
		container.FollowOutput(printer)
		container.StartLogProducer(ctx)
	}

	slog.Info("container started, waiting for it to be ready")

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		return err
	}

	runner, err := mutations.NewRunner(ctx, uri, nil)
	if err != nil {
		return err
	}
	defer runner.Close(ctx)
	return runner.ApplyProject(ctx, project, mutations.Options{All: t.All})
}

package main

import (
	"context"
	"log/slog"

	"github.com/ceymard/dmut/v2/mutations"
)

type DownCmd struct {
	Uri       string `arg:"" help:"Database host."`
	Namespace string `arg:"" help:"Namespace to down."`
	Dry       bool   `short:"d" help:"Dry run, don't down the mutations."`
}

func (c DownCmd) Run(ctx context.Context) error {
	var fake_empty_local_set *mutations.MutationSet = mutations.NewMutationSet(c.Namespace, 0, "")

	runner, err := mutations.NewPgRunner(ctx, c.Uri, nil)
	if err != nil {
		return err
	}
	defer runner.Close(ctx)

	db_mutations, err := runner.GetDBMutationsFromDb(ctx, c.Namespace)
	if err != nil {
		return err
	}

	fake_empty_local_set.Revision = db_mutations.Revision

	if err := runner.Begin(ctx); err != nil {
		return err
	}

	if err := mutations.RunMutations(ctx, runner, fake_empty_local_set, &mutations.MutationRunnerOptions{
		Commit: !c.Dry,
	}); err != nil {
		return err
	}

	if c.Dry {
		slog.Info("rolling back, dry run")
		return runner.Rollback(ctx)
	}

	return runner.Commit(ctx)
}

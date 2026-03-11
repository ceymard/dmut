package main

import "github.com/ceymard/dmut/v2/mutations"

type DownCmd struct {
	Uri       string `arg:"" help:"Database host."`
	Verbose   bool   `short:"v" help:"Verbose output."`
	Namespace string `arg:"" help:"Namespace to down."`
	Dry       bool   `short:"d" help:"Dry run, don't down the mutations."`
}

func (c DownCmd) Run() error {
	var fake_empty_local_set *mutations.MutationSet = mutations.NewMutationSet(c.Namespace, 0, "")

	runner, err := mutations.NewPgRunner(c.Uri, c.Verbose)
	if err != nil {
		return err
	}
	defer runner.Close()

	db_mutations, err := runner.GetDBMutationsFromDb(c.Namespace)
	if err != nil {
		return err
	}

	fake_empty_local_set.Revision = db_mutations.Revision

	if err := runner.Begin(); err != nil {
		return err
	}

	if err := mutations.RunMutations(runner, fake_empty_local_set, &mutations.MutationRunnerOptions{
		Verbose: c.Verbose,
		Commit:  !c.Dry,
	}); err != nil {
		return err
	}

	if c.Dry {
		return runner.Rollback()
	}

	return runner.Commit()
}

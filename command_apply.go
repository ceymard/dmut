package main

import "github.com/ceymard/dmut/mutations"

type ApplyCmd struct {
	Uri      string   `arg:"" help:"Database host."`
	Paths    []string `arg:"" help:"Paths to apply."`
	Override bool     `short:"o" name:"override" help:"Save the mutations to the database, but don't run them."`
	Verbose  bool     `short:"v" help:"Verbose output."`
}

func (a ApplyCmd) Run() error {

	muts, err := mutations.LoadYamlMutations(a.Paths...)
	if err != nil {
		return err
	}

	runner, err := mutations.NewPgRunner(a.Uri, a.Verbose)
	if err != nil {
		return err
	}
	defer runner.Close()

	// Test before
	if err := mutations.RunAllMutations(runner, muts, &mutations.MutationRunnerOptions{
		Commit:   true,
		Override: a.Override,
	}); err != nil {
		return err
	}
	runner.Logger().Println("tests were successful")

	return nil
}

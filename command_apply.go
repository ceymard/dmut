package main

import "github.com/ceymard/dmut/mutations"

type ApplyCmd struct {
	Uri      string   `arg:"" help:"Database host."`
	Paths    []string `arg:"" help:"Paths to apply."`
	Override bool     `short:"o" name:"override" help:"Save the mutations to the database, but don't run them."`
	Verbose  bool     `short:"v" help:"Verbose output."`
	Dry      bool     `short:"d" help:"Dry run, don't apply the mutations."`
}

func (a ApplyCmd) Run() error {

	if err := runMutations(a.Uri, a.Paths, mutations.MutationRunnerOptions{
		Verbose:  a.Verbose,
		Commit:   !a.Dry,
		Override: a.Override,
	}); err != nil {
		return err
	}
	return nil
}

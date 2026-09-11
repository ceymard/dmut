package main

import (
	"github.com/ceymard/dmut/v2/mutations"
	"github.com/samber/oops"
)

type ApplyCmd struct {
	Uri      string   `arg:"" help:"Database host."`
	Paths    []string `arg:"" help:"Paths to apply."`
	Override bool     `short:"o" name:"override" help:"Save the mutations to the database, but don't run them."`
	Verbose  bool     `short:"v" help:"Verbose output."`
	Dry      bool     `short:"d" help:"Dry run, don't apply the mutations."`
	Full     bool     `short:"f" name:"full" help:"Try changes without downing dependants first, but down and rebuild them if that fails."`
	Simple   bool     `short:"s" name:"simple" help:"Refuse to run if core has any change that isn't a pure append."`
}

func (a ApplyCmd) Run() error {

	if a.Full && a.Simple {
		return oops.In("apply").Errorf("-f/--full and -s/--simple are mutually exclusive")
	}

	if err := mutations.ReadAndRunMutations(a.Uri, a.Paths, &mutations.MutationRunnerOptions{
		Verbose:  a.Verbose,
		Commit:   !a.Dry,
		Override: a.Override,
		Full:     a.Full,
		Simple:   a.Simple,
	}); err != nil {
		return err
	}
	return nil
}

package main

import "github.com/ceymard/dmut/mutations"

func runMutations(uri string, paths []string, opts mutations.MutationRunnerOptions) error {

	muts, err := mutations.LoadYamlMutations(paths...)
	if err != nil {
		return err
	}

	runner, err := mutations.NewPgRunner(uri, opts.Verbose)
	if err != nil {
		return err
	}
	defer runner.Close()

	// Test before
	if err := mutations.RunAllMutations(runner, muts, &opts); err != nil {
		return err
	}

	return nil
}

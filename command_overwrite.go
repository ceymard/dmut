package main

import "context"

// Overwrite the destination database with the definition provided in the yaml paths

type OverwriteCmd struct {
	Host  string   `arg:"" help:"Database host."`
	Paths []string `arg:"" help:"Paths to the mutation files"`
}

func (o OverwriteCmd) Run(ctx context.Context) error {
	return nil
}

package main

import (
	"context"

	"github.com/ceymard/dmut/v2/mutations"
)

type DownCmd struct {
	Uri       string `arg:"" help:"Database host."`
	Namespace string `arg:"" help:"Namespace to down."`
	Dry       bool   `short:"d" help:"Dry run, don't down the mutations."`
}

func (c DownCmd) Run(ctx context.Context) error {
	runner, err := mutations.NewRunner(ctx, c.Uri, nil)
	if err != nil {
		return err
	}
	defer runner.Close(ctx)
	return runner.DownNamespace(ctx, c.Namespace, !c.Dry)
}

package main

import (
	"context"
	"strings"

	"github.com/ceymard/dmut/v3/mutations"
	"github.com/ceymard/dmut/v3/mutations/model"
	"github.com/samber/oops"
)

type ApplyCmd struct {
	Uri      string   `arg:"" help:"Database host."`
	Paths    []string `arg:"" help:"Paths to apply."`
	Override bool     `short:"o" name:"override" help:"Save the mutations to the database, but don't run them."`
	Dry      bool     `short:"d" help:"Dry run, don't apply the mutations."`
	Force    bool     `short:"f" name:"force" help:"Try changes without downing dependants first, and reapply them if that fails."`
	Safe     bool     `short:"s" name:"safe" help:"Refuse to run if any change is not a pure append."`
	All      bool     `short:"a" name:"all" help:"On an empty database, replay every revision from the first instead of the highest."`
	Set      []string `name:"set" sep:"none" help:"Value of the setting dmut.values.N, as N=value. Can be repeated."`
}

func (a ApplyCmd) Run(ctx context.Context) error {
	if a.Force && a.Safe {
		return oops.In("apply").Errorf("-f/--force and -s/--safe are mutually exclusive")
	}
	values := map[string]string{}
	for _, set := range a.Set {
		name, value, ok := strings.Cut(set, "=")
		if !ok {
			return oops.In("apply").With("set", set).Errorf("--set wants N=value, got %q", set)
		}
		values[name] = value
	}
	mode := model.ModeNormal
	switch {
	case a.Force:
		mode = model.ModeForce
	case a.Safe:
		mode = model.ModeSafe
	}
	return mutations.Apply(ctx, a.Uri, a.Paths, mutations.Options{
		Mode:     mode,
		Commit:   !a.Dry,
		Override: a.Override,
		All:      a.All,
		Values:   values,
	})
}

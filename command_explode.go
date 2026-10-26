package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ceymard/dmut/v2/mutations"
	"github.com/ceymard/dmut/v2/mutations/model"
)

type ExplodeCmd struct {
	OutDir string   `required:"" name:"out-dir" help:"Output directory to write the mutations to."`
	Paths  []string `arg:"" help:"Paths to the mutation files"`
}

func (e ExplodeCmd) Run(ctx context.Context) error {
	project, err := model.LoadPaths(e.Paths...)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(e.OutDir, 0o755); err != nil {
		return err
	}
	return mutations.Explode(project, func(name, contents string) error {
		path := filepath.Join(e.OutDir, name)
		fmt.Println(path)
		return os.WriteFile(path, []byte(contents), 0o644)
	})
}

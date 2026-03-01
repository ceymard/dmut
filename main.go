// I need a parser which will allow me to
//   negate a token
//   get a token as a target

// nolint: govet
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/alecthomas/kong"
)

var VERSION = "1.0.0"

type CLI struct {
	Collect CollectCmd `cmd:"" help:"Collect all paths into a single yaml file."`
	Dry     DryCmd     `cmd:"" help:"Apply the mutations but don't commit them."`
	Apply   ApplyCmd   `cmd:"" help:"Apply the mutations to the database."`

	Version VersionCmd `cmd:"" help:"Show the version."`

	Test   TestCmd   `cmd:"" help:"Test the mutations on an empty test database that will be created on the fly."`
	Legacy LegacyCmd `cmd:"" help:"Extract a yaml from a legacy dmut system prior to version 1.0.0"`
	// Extract ExtractCmd `cmd:"" help:""`
}

type CollectCmd struct {
	Outfile string   `arg:"" help:"Output YAML file."`
	Paths   []string `arg:"" help:"Paths to collect."`
}

func (c CollectCmd) Run() error {
	_ = c.Outfile
	_ = c.Paths
	return nil
}

type ApplyCmd struct {
	Host  string   `arg:"" help:"Database host."`
	Paths []string `arg:"" help:"Paths to apply."`
}

type DryCmd struct {
	ApplyCmd
}

func (a ApplyCmd) Run() error {
	_ = a.Host
	_ = a.Paths
	return nil
}

type VersionCmd struct{}

func (VersionCmd) Run() error {
	fmt.Println(VERSION)
	return nil
}

func main() {
	cli := CLI{}
	ctx := kong.Parse(&cli,
		kong.Name("dmut"),
		kong.Description("Database mutation runner. Collect, test, and apply migrations."),
		kong.UsageOnError(),
	)
	if err := ctx.Run(); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

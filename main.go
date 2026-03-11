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
	"github.com/samber/oops"
)

var VERSION = "1.0.0"

type CLI struct {
	Collect CollectCmd `cmd:"" help:"Collect all paths into a single yaml file."`
	Apply   ApplyCmd   `cmd:"" help:"Apply the mutations to the database."`
	Down    DownCmd    `cmd:"" help:"Down the mutations from the database."`
	Version VersionCmd `cmd:"" help:"Show the version."`
	Explode ExplodeCmd `cmd:"" help:"Explode mutations into individual yaml files."`

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

type VersionCmd struct{}

func (VersionCmd) Run() error {
	fmt.Println(VERSION)
	return nil
}

func main() {
	cli := CLI{}
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	ctx := kong.Parse(&cli,
		kong.Name("dmut"),
		kong.Description("Database mutation runner. Collect, test, and apply migrations."),
		kong.UsageOnError(),
	)
	if err := ctx.Run(); err != nil {
		if oops_err, ok := err.(oops.OopsError); ok {
			fmt.Printf("%+v\n", oops_err)

			// fmt.Println(oops_err.Error())
			// fmt.Println(oops_err.Stacktrace())
		} else {
			fmt.Println(err)
		}
		os.Exit(1)
	}
}

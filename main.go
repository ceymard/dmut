// I need a parser which will allow me to
//   negate a token
//   get a token as a target

// nolint: govet
package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/alecthomas/kong"
	"github.com/ceymard/dmut/v2/mutations"
)

var VERSION = "1.0.0"

type CLI struct {
	LogFlags

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
	Outfile  string   `arg:"" help:"Output YAML file, or - for stdout."`
	Paths    []string `arg:"" help:"Paths to collect, or a single postgres:// URI to source from a live database (only with -r)."`
	Revision int      `short:"r" name:"revision" help:"Emit a revision shell at this number instead of a flat collect (see specs/changes.md, Squash revisions)."`
}

func (c CollectCmd) Run(ctx context.Context) error {
	out, close_out, err := c.openOutfile()
	if err != nil {
		return err
	}
	defer close_out()

	if c.Revision == 0 {
		muts, err := mutations.LoadYamlMutations(c.Paths...)
		if err != nil {
			return err
		}
		return mutations.Collect(muts, out)
	}

	var shells []*mutations.MutationSet
	var warnings []string

	if len(c.Paths) == 1 && isPostgresURI(c.Paths[0]) {
		runner, err := mutations.NewPgRunner(ctx, c.Paths[0], nil)
		if err != nil {
			return err
		}
		defer runner.Close(ctx)

		shells, warnings, err = mutations.CollectRevisionFromDatabase(ctx, runner, c.Revision)
		if err != nil {
			return err
		}
	} else {
		muts, err := mutations.LoadYamlMutations(c.Paths...)
		if err != nil {
			return err
		}
		shells, warnings, err = mutations.CollectRevisionFromLocal(muts, c.Revision)
		if err != nil {
			return err
		}
	}

	for _, w := range warnings {
		slog.Warn(w)
	}

	return mutations.CollectRevisionShell(shells, out)
}

// openOutfile returns where to write the collected yaml (stdout for "-", a created
// file otherwise) and a close function the caller must always defer.
func (c CollectCmd) openOutfile() (io.Writer, func() error, error) {
	if c.Outfile == "-" {
		return os.Stdout, func() error { return nil }, nil
	}

	f, err := os.Create(c.Outfile)
	if err != nil {
		return nil, nil, err
	}
	return f, f.Close, nil
}

// isPostgresURI reports whether s looks like a postgres connection URI rather than a
// filesystem path - used only to decide `collect -r`'s source, since every other
// command that takes a database URI has a dedicated argument for it.
func isPostgresURI(s string) bool {
	return strings.HasPrefix(s, "postgres://") || strings.HasPrefix(s, "postgresql://")
}

type VersionCmd struct{}

func (VersionCmd) Run(context.Context) error {
	fmt.Println(VERSION)
	return nil
}

func main() {
	// Ctrl-C cancels the context: the run stops and rolls back instead of the
	// process being killed mid-statement.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cli := CLI{}
	kctx := kong.Parse(&cli,
		kong.BindTo(ctx, (*context.Context)(nil)),
		kong.Name("dmut"),
		kong.Description("Database mutation runner. Collect, test, and apply migrations."),
		kong.UsageOnError(),
	)
	logger := cli.NewLogger(os.Stderr)
	slog.SetDefault(logger)
	if err := kctx.Run(); err != nil {
		cli.reportError(logger, os.Stderr, err)
		stop()
		os.Exit(1)
	}
}

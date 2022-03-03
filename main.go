// I need a parser which will allow me to
//   negate a token
//   get a token as a target

// nolint: govet
package main

import (
	"log"

	"github.com/alecthomas/kong"
	"github.com/ceymard/dmut/mutations"

	_ "github.com/lib/pq"
)

var cli struct {
	AST   bool               `help:"Print AST for expression."`
	Set   map[string]float64 `short:"s" help:"Set variables."`
	Host  string             `arg required help:"postgres or sqlite uri"`
	Files []string           `arg required help:"Files to include"`
}

func main() {
	kong.Parse(&cli,
		kong.Description("Run mutations on databases"),
		kong.UsageOnError(),
	)

	// expr := &TopLevel{}
	if err := mutations.ParseAndRunMutations(cli.Host, cli.Files...); err != nil {
		log.Print(err)
		// if cli.AST {
		// 	// } else {
		// 	pp.PrintMapTypes = false
		// 	for _, mut := range muts {
		// 		_, _ = fmt.Printf("\n\n---- %s ----- \n", mut.Name)
		// 		pp.Print(map[string]interface{}{"Name": mut.Name, "Up": mut.Up, "Down": mut.Down, "Hash": mut.Hash})
		// 	}
		// 	// fmt.Println(expr)
		// }

	}
}

// I need a parser which will allow me to
//   negate a token
//   get a token as a target

// nolint: govet
package main

import (
	"fmt"
	"log"

	"github.com/alecthomas/kong"
	"github.com/ceymard/dmut/mutations"
	"github.com/k0kubun/pp"

	_ "github.com/lib/pq"
)

var cli struct {
	AST   bool               `help:"Print AST for expression."`
	Set   map[string]float64 `short:"s" help:"Set variables."`
	Host  string             `arg required help:"postgres uri"`
	Files []string           `arg required help:"Files to include"`
}

func main() {
	kong.Parse(&cli,
		kong.Description("A basic expression parser and evaluator."),
		kong.UsageOnError(),
	)

	// expr := &TopLevel{}
	for _, filename := range cli.Files {
		mp, err := mutations.GetMutationMapFromFile(filename)
		if err != nil {
			_, _ = fmt.Print(err)
			// continue
		} else {
			var mutsOrig mutations.Mutations = (*mp).GetInOrder()
			var muts = append([]*mutations.Mutation{}, mutations.DmutMutations...)
			muts = append(muts, mutsOrig...)

			if cli.AST {
				// } else {
				pp.PrintMapTypes = false
				for _, mut := range muts {
					_, _ = fmt.Printf("\n\n---- %s ----- \n", mut.Name)
					pp.Print(map[string]interface{}{"Name": mut.Name, "Up": mut.Up, "Down": mut.Down, "Hash": mut.Hash})
				}
				// fmt.Println(expr)
			}

			// now that we have
			if err = mutations.RunMutations(cli.Host, muts); err != nil {
				log.Print(err)
			}
		}

	}
}

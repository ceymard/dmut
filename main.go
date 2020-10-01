// I need a parser which will allow me to
//   negate a token
//   get a token as a target

// nolint: govet
package main

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"

	"github.com/alecthomas/kong"
	"github.com/ceymard/dmut/mutations"
	dmutparser "github.com/ceymard/dmut/parser"
	"github.com/k0kubun/pp"

	_ "github.com/lib/pq"
)

var cli struct {
	AST   bool               `help:"Print AST for expression."`
	Set   map[string]float64 `short:"s" help:"Set variables."`
	Files []string           `arg required help:"Files to include"`
}

func ParseFile(name string) (*dmutparser.TopLevel, error) {
	reader, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	expr := &dmutparser.TopLevel{}
	err = dmutparser.Parser.Parse("", reader, expr)
	return expr, err
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
					pp.Print(map[string]interface{}{"Name": mut.Name, "Up": mut.Up, "Down": mut.Down, "Hash": hex.EncodeToString(mut.Hash())})
				}
				// fmt.Println(expr)
			}

			// now that we have
			if err = mutations.RunMutations(muts); err != nil {
				log.Print(err)
			}
		}

	}
}

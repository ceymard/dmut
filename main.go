// I need a parser which will allow me to
//   negate a token
//   get a token as a target

// nolint: govet
package main

import (
	"fmt"
	"os"

	"github.com/alecthomas/kong"
	"github.com/ceymard/gomut/mutations"
	dmutparser "github.com/ceymard/gomut/parser"
	"github.com/k0kubun/pp"
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
	err = dmutparser.Parser.Parse(reader, expr)
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
			fmt.Print(err)
			// continue
		} else {
			if cli.AST {
				// } else {
				for name, mut := range *mp {
					fmt.Printf("--- %s --- \n", name)
					pp.PrintMapTypes = false
					pp.Print(map[string][]string{"Up": mut.Up, "Down": mut.Down})
				}
				// fmt.Println(expr)
			}

			fmt.Print("ok. ")

		}

	}
}

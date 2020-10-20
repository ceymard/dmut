package mutations

import (
	"fmt"
	"os"

	dmutparser "github.com/ceymard/dmut/parser"
	"github.com/flosch/pongo2/v4"
	"github.com/pkg/errors"
)

func init() {
	pongo2.SetAutoescape(false)
	pongo2.Globals["env"] = os.Getenv
}

func GetMutationsInFile(filename string, set *MutationSet) error {
	// Send empty context.
	contents, err := pongo2.RenderTemplateFile(filename, pongo2.Context{})
	if err != nil {
		return fmt.Errorf("in %s, %w", filename, err)
		// return err
	}

	// contents, err = runTemplate(filename, contents, set)
	// if err != nil {
	// }
	root, err := dmutparser.ParseString(filename, contents)
	if err != nil {
		return fmt.Errorf("in %s, %w", filename, err)
	}

	if root.Decls != nil {
		for _, astmut := range *root.Decls {
			var mut = NewMutation(*astmut.Name, astmut.DependsOn, nil)
			for _, stmt := range *astmut.Statements {
				mut.AddDown(stmt.Down())
				mut.AddUp(stmt.Up(contents))
			}
			(*set)[mut.Name] = mut
		}
	}

	if err != nil {
		return err
	}

	return nil
}

func GetMutationMapFromFile(filename string) (*MutationSet, error) {
	var set = make(MutationSet)
	err := GetMutationsInFile(filename, &set)
	if err != nil {
		return nil, err
	}

	// Now, apply the parent / child logic
	for _, mut := range set {
		if mut.DependsOn != nil {
			for _, dep := range *mut.DependsOn {
				parent := set[dep]
				if parent == nil {
					return nil, errors.Errorf("mutation '%s' requests an inexistent mutation '%s'", mut.Name, dep)
				}
				// FIXME should probably detect cycles here ?
				mut.AddParent(parent)
			}
		}
	}

	for _, mut := range set {
		_, err = mut.ComputeHash()
		if err != nil {
			return nil, errors.Errorf("mutation '%s' hash error %w", mut.Name, err)
		}
	}

	return &set, nil
}

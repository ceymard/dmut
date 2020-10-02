package mutations

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"

	dmutparser "github.com/ceymard/dmut/parser"
	"github.com/pkg/errors"
)

func readAll(filename string) (string, error) {
	var file, err = os.Open(filename)
	if err != nil {
		return "", err
	}
	contents, err := ioutil.ReadAll(file)
	if err != nil {
		return "", err
	}
	return string(contents), nil
}

func GetMutationsInFile(filename string, set *MutationSet) error {
	contents, err := readAll(filename)
	if err != nil {
		return err
	}

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

	if root.Includes != nil {
		for _, incl := range *root.Includes {
			var pth = *incl.Path
			var name = pth[1 : len(pth)-1]
			err = GetMutationMapFromInclude(filename, name, set)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func GetMutationMapFromInclude(origFilename string, relName string, set *MutationSet) error {
	var dirname = path.Dir(origFilename)
	var newname = path.Join(dirname, relName)
	return GetMutationsInFile(newname, set)
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

	return &set, nil
}

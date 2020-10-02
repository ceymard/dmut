package mutations

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"text/template"

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

type tplenv struct{}

func runTemplate(infile string, cts string, set *MutationSet) string {
	var (
		tpl = template.New("stmt").Funcs(template.FuncMap{
			"env": func(name string) string {
				return os.Getenv(name)
			},
			"include": func(pth string) string {
				var dirname = path.Dir(infile)
				var newname = path.Join(dirname, pth)
				if err := GetMutationsInFile(newname, set); err != nil {
					panic(err)
				}
				return ""
			},
		})
		err error
	)
	var buf bytes.Buffer
	if tpl, err = tpl.Parse(cts); err != nil {
		panic(err)
	}

	if err = tpl.Execute(&buf, tplenv{}); err != nil {
		panic(err)
	}
	return buf.String()
}

func GetMutationsInFile(filename string, set *MutationSet) error {
	contents, err := readAll(filename)
	if err != nil {
		return err
	}

	contents = runTemplate(filename, contents, set)
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

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

var tpl = template.New("stmt")

func runTemplate(infile string, cts string, set *MutationSet) (string, error) {
	var (
		err      error
		prevtree = tpl.Tree
	)

	tpl.Funcs(template.FuncMap{
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
		"arr": func(values ...interface{}) []interface{} {
			return values
		},
		"dict": func(values ...interface{}) (map[string]interface{}, error) {
			if len(values)%2 != 0 {
				return nil, errors.New("invalid dict call")
			}
			dict := make(map[string]interface{}, len(values)/2)
			for i := 0; i < len(values); i += 2 {
				key, ok := values[i].(string)
				if !ok {
					return nil, errors.New("dict keys must be strings")
				}
				dict[key] = values[i+1]
			}
			return dict, nil
		},
	})

	var buf bytes.Buffer
	if tpl, err = tpl.Parse(cts); err != nil {
		return "", err
	}

	if prevtree != tpl.Tree {
		if err = tpl.Execute(&buf, tplenv{}); err != nil {
			return "", err
		}
	}
	return buf.String(), nil
}

func GetMutationsInFile(filename string, set *MutationSet) error {
	contents, err := readAll(filename)
	if err != nil {
		return err
	}

	contents, err = runTemplate(filename, contents, set)
	if err != nil {
		return fmt.Errorf("in %s, %w", filename, err)
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

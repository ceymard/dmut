package mutations

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/goccy/go-yaml"
)

type MutationMap map[string]*Mutation

func readMutations(file *MutationSet, mutations MutationMap, parent *Mutation) error {
	for name, mut := range mutations {

		if parent != nil {
			mut.Name = parent.Name + "." + name
		} else {
			mut.Name = name
		}

		if err := file.AddMutation(mut); err != nil {
			return err
		}

		if len(mut.ChildrenMutations) > 0 {
			if err := readMutations(file, mut.ChildrenMutations, mut); err != nil {
				return err
			}
		}

	}

	return nil
}

func (ms *MutationSet) readFile(filename string) error {
	ms.File = filename
	f, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error reading file %s: %w", filename, err)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	for {
		var ym MutationMap = make(MutationMap)
		err = dec.Decode(&ym)
		if err == io.EOF {
			break // normal end of stream
		}

		if err != nil {
			return fmt.Errorf("error decoding yaml: %w", err)
		}

		if err := readMutations(ms, ym, nil); err != nil {
			return err
		}
	}
	return nil
}

func LoadYamlMutations(paths ...string) (MutationNamespace, error) {
	var res MutationNamespace = make(MutationNamespace)

	// var res = &YamlMigrationSet{
	// 	Mutations: make(MutationMap),
	// 	Roles:     mapset.NewSet[string](),
	// 	Namespace: "",
	// 	Revision:  0,
	// }
	for _, path := range paths {
		mut_file := NewMutationSet("", 0, path)

		if info, err := os.Stat(path); err != nil {
			return nil, err
		} else if info.IsDir() {
			files, err := os.ReadDir(path)
			if err != nil {
				return nil, err
			}
			for _, file := range files {
				if strings.HasPrefix(file.Name(), "_") {
					continue
				}

				if file.IsDir() {
					if err := mut_file.readFile(filepath.Join(path, file.Name())); err != nil {
						return nil, err
					}
					continue
				}

				if !strings.HasSuffix(file.Name(), ".yaml") && !strings.HasSuffix(file.Name(), ".yml") {
					continue
				}

				err = mut_file.readFile(filepath.Join(path, file.Name()))
				if err != nil {
					return nil, err
				}
			}
		} else {
			if !strings.HasSuffix(path, ".yaml") && !strings.HasSuffix(path, ".yml") {
				continue
			}
			err = mut_file.readFile(path)
			if err != nil {
				return nil, err
			}
		}

		if err := res.AddSet(mut_file); err != nil {
			return nil, err
		}
	}

	if err := res.ResolveDependencies(); err != nil {
		return nil, err
	}

	if err := res.EnsureContinuousRevisions(); err != nil {
		return nil, err
	}

	return res, nil
}

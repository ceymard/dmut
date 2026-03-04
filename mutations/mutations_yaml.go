package mutations

import (
	"embed"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/goccy/go-yaml"
)

//go:embed dmut-mutations/*
var dmut_mutations embed.FS

type MutationMap map[string]*Mutation

func (ms *MutationSet) readFile(system fs.FS, filename string) error {
	ms.File = filename
	f, err := system.Open(filename)
	if err != nil {
		return fmt.Errorf("error reading file %s: %w", filename, err)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	for {
		var mp = make(map[string]interface{})

		err = dec.Decode(&mp)
		if err == io.EOF {
			break // normal end of stream
		}

		if err != nil {
			return fmt.Errorf("error decoding yaml: %w", err)
		}

		for key, value := range mp {
			switch key {
			case "__namespace":
				if v, ok := value.(string); ok {
					ms.Namespace = v
				} else {
					return fmt.Errorf("__namespace must be a string")
				}
			case "__revision":
				if v, ok := value.(int); ok {
					ms.Revision = v
				} else {
					return fmt.Errorf("__revision must be an integer")
				}
			default:
				if mut, err := parseMutation(ms, value); err != nil {
					return err
				} else {
					ms.AddMutation(mut)
				}
			}
		}

	}
	return nil
}

func readFile(namespace *MutationNamespace, system fs.FS, filename string) error {
	if !strings.HasSuffix(filename, ".yaml") && !strings.HasSuffix(filename, ".yml") {
		return nil
	}

	ms := NewMutationSet("", 0, filename)
	if err := ms.readFile(system, filename); err != nil {
		return err
	}
	namespace.AddSet(ms)
	return nil
}

func browseFs(namespace *MutationNamespace, system fs.FS, root string) error {
	entries, err := fs.ReadDir(system, root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			if err := browseFs(namespace, system, filepath.Join(root, entry.Name())); err != nil {
				return err
			}
		} else {
			if err := readFile(namespace, system, filepath.Join(root, entry.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func LoadYamlMutations(paths ...string) (*MutationNamespace, error) {
	var res = NewMutationNamespace()

	if err := browseFs(res, dmut_mutations, "."); err != nil {
		return nil, err
	}

	for _, path := range paths {
		if info, err := os.Stat(path); err != nil {
			return nil, err
		} else if info.IsDir() {

			dirfs := os.DirFS(path)
			if err := browseFs(res, dirfs, "."); err != nil {
				return nil, err
			}

		} else {
			dirfs := os.DirFS(filepath.Dir(path))
			fname := filepath.Base(path)
			if err := readFile(res, dirfs, fname); err != nil {
				return nil, err
			}
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

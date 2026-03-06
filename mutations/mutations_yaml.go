package mutations

import (
	"embed"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
	"github.com/samber/oops"
)

//go:embed dmut-mutations/*
var dmut_mutations embed.FS

type MutationMap map[string]*Mutation

func (ms *MutationSet) readFile(system fs.FS, filename string) error {
	ms.File = filename
	f, err := system.Open(filename)
	if err != nil {
		return oops.In("mutations").With("filename", filename).Wrapf(err, "error reading file %s", filename)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	for {
		// var mp = make(map[string]interface{})
		var node ast.Node

		err = dec.Decode(&node)
		if err == io.EOF {
			break // normal end of stream
		}

		map_node, ok := node.(*ast.MappingNode)
		if !ok {
			return oops.In("mutations").With("filename", filename).Errorf("expected a mapping node, got %T", node)
		}

		for _, mapping := range map_node.Values {
			key_node := mapping.Key
			var key string
			if err := yaml.NodeToValue(key_node, &key); err != nil {
				return oops.In("mutations").With("filename", filename).Wrapf(err, "error decoding key %T", key_node)
			}
			value := mapping.Value

			switch key {
			case "__namespace":
				var namespace string
				if err := yaml.NodeToValue(value, &namespace); err != nil {
					return oops.In("mutations").With("filename", filename).Wrapf(err, "error decoding __namespace %T", value)
				}
				ms.Namespace = namespace
			case "__revision":
				var revision int
				if err := yaml.NodeToValue(value, &revision); err != nil {
					return oops.In("mutations").With("filename", filename).Wrapf(err, "error decoding __revision %T", value)
				}
				ms.Revision = revision
			case "__override":
				var override bool
				if err := yaml.NodeToValue(value, &override); err != nil {
					return oops.In("mutations").With("filename", filename).Wrapf(err, "error decoding __override %T", value)
				}
				ms.Override = override
			default:
				if _, err := parseMutation(key, ms, value); err != nil {
					return err
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

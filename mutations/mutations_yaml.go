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

// readDocument reads a single yaml document into the set. Each document carries its
// own __namespace and __revision, so a file may hold several of them.
func (ms *MutationSet) readDocument(node ast.Node) error {
	filename := ms.File
	oo := oops.In("mutations").With("filename", filename)

	map_node, ok := node.(*ast.MappingNode)
	if !ok {
		return oo.Errorf("expected a mapping node, got %T", node)
	}

	// A document is override-shaped if it declares any of mode/delete/override/mutations -
	// in which case mutation definitions must live nested under mutations:, not as flat
	// top-level keys, so a mutation can never collide with one of these reserved names.
	// Scanned up front so this doesn't depend on where in the file the reserved keys
	// happen to appear relative to any (invalid, in that case) flat mutation name.
	overrideShaped := false
	for _, mapping := range map_node.Values {
		var key string
		if err := yaml.NodeToValue(mapping.Key, &key); err != nil {
			return oo.Wrapf(err, "error decoding key %T", mapping.Key)
		}
		switch key {
		case "mode", "delete", "override", "mutations":
			overrideShaped = true
		}
	}
	ms.OverrideShaped = overrideShaped

	for _, mapping := range map_node.Values {
		key_node := mapping.Key
		var key string
		if err := yaml.NodeToValue(key_node, &key); err != nil {
			return oo.Wrapf(err, "error decoding key %T", key_node)
		}
		value := mapping.Value

		switch key {
		case "__namespace":
			var namespace string
			if err := yaml.NodeToValue(value, &namespace); err != nil {
				return oo.Wrapf(err, "error decoding __namespace %T", value)
			}
			ms.Namespace = namespace
		case "__revision":
			var revision int
			if err := yaml.NodeToValue(value, &revision); err != nil {
				return oo.Wrapf(err, "error decoding __revision %T", value)
			}
			ms.Revision = revision
		case "mode":
			var mode string
			if err := yaml.NodeToValue(value, &mode); err != nil {
				return oo.Wrapf(err, "error decoding mode %T", value)
			}
			switch mode {
			case "default", "full", "simple":
				ms.Mode = mode
			default:
				return oo.Errorf("unknown mode %q, expected one of default, full, simple", mode)
			}
		case "delete":
			if list, err := parseStringList(value); err != nil {
				return err
			} else {
				ms.Delete = list
			}
		case "override":
			override_def, ok := value.(*ast.MappingNode)
			if !ok {
				return oo.Errorf("'override' must be a map of mutations, got %T", value)
			}
			// A separate set, not ms itself: override entries are recorded, never
			// executed, so they must not join ms's own dependency graph or collide
			// with mutations: entries of the same name.
			ms.OverrideMutations = NewMutationSet(ms.Namespace, ms.Revision, ms.File)
			for _, entry := range override_def.Values {
				var name string
				if err := yaml.NodeToValue(entry.Key, &name); err != nil {
					return oo.Wrapf(err, "error decoding override mutation name %T", entry.Key)
				}
				if _, err := parseMutation(name, ms.OverrideMutations, entry.Value); err != nil {
					return err
				}
			}
		case "mutations":
			mutations_def, ok := value.(*ast.MappingNode)
			if !ok {
				return oo.Errorf("'mutations' must be a map of mutations, got %T", value)
			}
			for _, entry := range mutations_def.Values {
				var name string
				if err := yaml.NodeToValue(entry.Key, &name); err != nil {
					return oo.Wrapf(err, "error decoding mutation name %T", entry.Key)
				}
				if _, err := parseMutation(name, ms, entry.Value); err != nil {
					return err
				}
			}
		default:
			if overrideShaped {
				return oo.Errorf("unknown key '%s' - a revision declaring mode/delete/override/mutations must nest its mutation definitions under 'mutations:'", key)
			}
			if _, err := parseMutation(key, ms, value); err != nil {
				return err
			}
		}
	}

	// __namespace and __revision may appear after the mutations they apply to.
	for mut := range ms.AllMutations() {
		mut.Namespace = ms.Namespace
	}
	if ms.OverrideMutations != nil {
		for mut := range ms.OverrideMutations.AllMutations() {
			mut.Namespace = ms.Namespace
		}
	}

	return nil
}

func readFile(namespace *MutationNamespace, system fs.FS, filename string) error {
	if !strings.HasSuffix(filename, ".yaml") && !strings.HasSuffix(filename, ".yml") {
		return nil
	}

	f, err := system.Open(filename)
	if err != nil {
		return oops.In("mutations").With("filename", filename).Wrapf(err, "error reading file %s", filename)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	for {
		var node ast.Node

		err = dec.Decode(&node)
		if err == io.EOF {
			break // normal end of stream
		}
		if err != nil {
			return oops.In("mutations").With("filename", filename).Wrapf(err, "error decoding file %s", filename)
		}

		ms := NewMutationSet("", 0, filename)
		if err := ms.readDocument(node); err != nil {
			return err
		}
		if err := namespace.AddSet(ms); err != nil {
			return err
		}
	}

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

package model

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/goccy/go-yaml"
	"github.com/samber/oops"
)

// LoadPaths reads yaml files and directories (recursively) into a Project.
// Revisions are checked for gaps, and dependencies are resolved. Files that are not
// .yml or .yaml are ignored.
func LoadPaths(paths ...string) (*Project, error) {
	return Load(nil, paths...)
}

// Load is LoadPaths preceded by the yaml files under the root of each fsys, which come first
// in the namespace order.
func Load(fsyss []fs.FS, paths ...string) (*Project, error) {
	b := newBuilder()
	for _, fsys := range fsyss {
		if err := b.readDir(fsys, ".", ""); err != nil {
			return nil, err
		}
	}
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			return nil, oops.In("model").With("path", p).Wrap(err)
		}
		if info.IsDir() {
			err = b.readDir(os.DirFS(p), ".", p)
		} else {
			err = b.readFile(os.DirFS(filepath.Dir(p)), filepath.Base(p), p)
		}
		if err != nil {
			return nil, err
		}
	}
	return b.finish()
}

// LoadFS reads the yaml files under the root of fsys into a Project.
func LoadFS(fsys fs.FS) (*Project, error) {
	return Load([]fs.FS{fsys})
}

type nsBuild struct {
	name     string
	explicit map[int]*Revision
	unnamed  *Revision
}

type builder struct {
	order      []string
	namespaces map[string]*nsBuild
	warnings   []Warning
}

func newBuilder() *builder {
	return &builder{namespaces: map[string]*nsBuild{}}
}

func (b *builder) namespace(name string) *nsBuild {
	ns, ok := b.namespaces[name]
	if !ok {
		ns = &nsBuild{name: name, explicit: map[int]*Revision{}}
		b.namespaces[name] = ns
		b.order = append(b.order, name)
	}
	return ns
}

// readDir reads the files under dir; root is the path to show for the root of fsys.
func (b *builder) readDir(fsys fs.FS, dir, root string) error {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return oops.In("model").With("path", filepath.Join(root, dir)).Wrap(err)
	}
	for _, e := range entries {
		name := path.Join(dir, e.Name())
		if e.IsDir() {
			err = b.readDir(fsys, name, root)
		} else {
			err = b.readFile(fsys, name, filepath.Join(root, name))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *builder) readFile(fsys fs.FS, name, shown string) error {
	ext := path.Ext(name)
	if ext != ".yml" && ext != ".yaml" {
		return nil
	}
	f, err := fsys.Open(name)
	if err != nil {
		return oops.In("model").With("file", shown).Wrap(err)
	}
	defer f.Close()

	// The name of a mutation file is its name without extension, wherever it sits.
	base := strings.TrimSuffix(path.Base(name), ext)
	dec := yaml.NewDecoder(f, yaml.UseOrderedMap())
	documents := 0
	for {
		var doc any
		if err := dec.Decode(&doc); errors.Is(err, io.EOF) {
			if documents == 0 {
				(where{file: shown, warnings: &b.warnings}).warn("empty yaml file, ignored")
			}
			return nil
		} else if err != nil {
			return oops.In("model").With("file", shown).Wrapf(err, "%s: can't read yaml", shown)
		}
		documents++
		w := where{file: shown, warnings: &b.warnings}
		if doc == nil {
			w.warn("empty yaml document, ignored")
			continue
		}
		m, ok := doc.(yaml.MapSlice)
		if !ok {
			return w.errf("a document must be a mapping, got %T", doc)
		}
		var err error
		if len(m) > 0 && m[0].Key == "revision" {
			err = b.addRevisionDocument(w, m)
		} else {
			err = b.addMutationFile(w, base, m)
		}
		if err != nil {
			return err
		}
	}
}

// addMutationFile reads a document that holds the one mutation named after its file.
func (b *builder) addMutationFile(w where, name string, m yaml.MapSlice) error {
	namespace := ""
	rest := make(yaml.MapSlice, 0, len(m))
	for _, item := range m {
		if item.Key == "namespace" {
			s, ok := item.Value.(string)
			if !ok {
				return w.errf("namespace must be a string, got %T", item.Value)
			}
			namespace = s
			continue
		}
		rest = append(rest, item)
	}
	mut, err := parseMutation(w, name, namespace, rest)
	if err != nil {
		return err
	}
	ns := b.namespace(namespace)
	if ns.unnamed == nil {
		ns.unnamed = &Revision{Namespace: namespace, byName: map[string]*Mutation{}}
	}
	return ns.unnamed.add(w, mut)
}

// addRevisionDocument reads a document whose first key is revision.
func (b *builder) addRevisionDocument(w where, m yaml.MapSlice) error {
	var number int
	var namespace string
	var mode Mode
	var del []string
	var overrideRaw, mutationsRaw yaml.MapSlice
	for _, item := range m {
		key, _ := item.Key.(string)
		switch key {
		case "revision":
			n, ok := asInt(item.Value)
			if !ok || n < 1 {
				return w.errf("revision must be a positive integer, got %v", item.Value)
			}
			number = n
		case "namespace":
			s, ok := item.Value.(string)
			if !ok {
				return w.errf("namespace must be a string, got %T", item.Value)
			}
			namespace = s
		case "mode":
			s, _ := item.Value.(string)
			switch Mode(s) {
			case ModeSafe, ModeNormal, ModeForce:
				mode = Mode(s)
			default:
				return w.errf("unknown mode %v ; expected safe, normal or force", item.Value)
			}
		case "delete":
			list, ok := item.Value.([]any)
			if !ok {
				return w.errf("delete must be a list of mutation names, got %T", item.Value)
			}
			for _, e := range list {
				s, ok := e.(string)
				if !ok {
					return w.errf("delete must hold mutation names, got %T", e)
				}
				del = append(del, s)
			}
		case "override", "mutations":
			sub, ok := item.Value.(yaml.MapSlice)
			if !ok && item.Value != nil {
				return w.errf("%s must be a mapping of mutations, got %T", key, item.Value)
			}
			if key == "override" {
				overrideRaw = sub
			} else {
				mutationsRaw = sub
			}
		default:
			return w.errf("unknown key %v in a revision document", item.Key)
		}
	}

	ns := b.namespace(namespace)
	rev, ok := ns.explicit[number]
	if !ok {
		rev = &Revision{Namespace: namespace, Number: number, byName: map[string]*Mutation{}}
		ns.explicit[number] = rev
	}
	if mode != "" {
		if rev.Mode != "" && rev.Mode != mode {
			return w.errf("revision %d is given two modes: %s and %s", number, rev.Mode, mode)
		}
		rev.Mode = mode
	}
	rev.Delete = append(rev.Delete, del...)

	for _, item := range mutationsRaw {
		mut, err := b.namedMutation(w, item, namespace)
		if err != nil {
			return err
		}
		if err := rev.add(w, mut); err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	for _, mut := range rev.Override {
		seen[mut.Name] = true
	}
	for _, item := range overrideRaw {
		mut, err := b.namedMutation(w, item, namespace)
		if err != nil {
			return err
		}
		if seen[mut.Name] {
			return w.errf("mutation %s is overridden twice", mut.Name)
		}
		seen[mut.Name] = true
		rev.Override = append(rev.Override, mut)
	}
	return nil
}

func (b *builder) namedMutation(w where, item yaml.MapItem, namespace string) (*Mutation, error) {
	name, ok := item.Key.(string)
	if !ok {
		return nil, w.errf("a mutation name must be a string, got %T", item.Key)
	}
	return parseMutation(w, name, namespace, item.Value)
}

func asInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case uint64:
		return int(n), true
	}
	return 0, false
}

func (r *Revision) add(w where, mut *Mutation) error {
	if prev, ok := r.byName[mut.Name]; ok {
		return w.errf("mutation %s is defined twice in namespace %q: here and in %s", mut.Name, r.Namespace, prev.File)
	}
	r.byName[mut.Name] = mut
	r.Mutations = append(r.Mutations, mut)
	return nil
}

// finish numbers the unnamed revisions, checks for gaps and resolves dependencies.
func (b *builder) finish() (*Project, error) {
	p := &Project{Warnings: b.warnings}
	for _, name := range b.order {
		src := b.namespaces[name]
		ns := &Namespace{Name: name, Revisions: src.explicit}
		for n := range ns.Revisions {
			if ns.Min == 0 || n < ns.Min {
				ns.Min = n
			}
			ns.Max = max(ns.Max, n)
		}
		if len(ns.Revisions) > 0 && len(ns.Revisions) != ns.Max-ns.Min+1 {
			return nil, oops.In("model").With("namespace", name).
				Errorf("namespace %q: revisions %d to %d have a gap", name, ns.Min, ns.Max)
		}
		if src.unnamed != nil {
			src.unnamed.Number = ns.Max + 1
			ns.Revisions[src.unnamed.Number] = src.unnamed
			ns.Max = src.unnamed.Number
			if ns.Min == 0 {
				ns.Min = src.unnamed.Number
			}
		}
		for _, rev := range ns.Revisions {
			if err := rev.Resolve(); err != nil {
				return nil, err
			}
		}
		p.Namespaces = append(p.Namespaces, ns)
	}
	return p, nil
}

package model

import (
	"slices"
	"sort"
	"strings"

	"github.com/samber/oops"
)

// Mutation returns the named mutation of the revision.
func (r *Revision) Mutation(name string) (*Mutation, bool) {
	m, ok := r.byName[name]
	return m, ok
}

// Resolve fills Parents and Matched of every mutation and checks the dependency graph
// for missing names and cycles. Override entries are resolved against the mutations that
// will be recorded: the mutations not deleted, with the overrides in their place.
func (r *Revision) Resolve() error {
	for _, m := range r.Mutations {
		if err := resolveMutation(m, r.byName, r.Mutations, r); err != nil {
			return err
		}
	}
	if _, err := r.UpOrder(); err != nil {
		return err
	}
	if len(r.Override) == 0 {
		return nil
	}
	deleted := map[string]bool{}
	for _, name := range r.Delete {
		deleted[name] = true
	}
	effective := map[string]*Mutation{}
	for _, m := range r.Mutations {
		if !deleted[m.Name] {
			effective[m.Name] = m
		}
	}
	for _, o := range r.Override {
		if !deleted[o.Name] {
			effective[o.Name] = o
		}
	}
	all := make([]*Mutation, 0, len(effective))
	for _, m := range effective {
		all = append(all, m)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })
	for _, o := range r.Override {
		if err := resolveMutation(o, effective, all, r); err != nil {
			return err
		}
	}
	return nil
}

// resolveMutation sets Parents and Matched of m from the mutations in byName and all.
func resolveMutation(m *Mutation, byName map[string]*Mutation, all []*Mutation, r *Revision) error {
	w := where{file: m.File, mutation: m.Name}
	m.Parents = nil
	for i := len(m.Name); i > 0; i-- {
		if m.Name[i-1] != '.' {
			continue
		}
		if parent, ok := byName[m.Name[:i-1]]; ok {
			m.Parents = append(m.Parents, parent)
		}
	}
	m.Matched = make([][]*Mutation, len(m.Deps))
	for i, entry := range m.Deps {
		if !isPattern(entry.Pattern) {
			dep, ok := byName[entry.Pattern]
			if !ok {
				return w.errf("depends on %s, which does not exist in revision %d of namespace %q", entry.Pattern, r.Number, r.Namespace)
			}
			m.Matched[i] = []*Mutation{dep}
			continue
		}
		for _, other := range all {
			if other != m && matchPattern(entry.Pattern, other.Name) {
				m.Matched[i] = append(m.Matched[i], other)
			}
		}
		sort.Slice(m.Matched[i], func(a, b int) bool { return m.Matched[i][a].Name < m.Matched[i][b].Name })
	}
	return nil
}

// Dependencies returns the distinct mutations m depends on, sorted by name.
func (m *Mutation) Dependencies() []*Mutation {
	seen := map[*Mutation]bool{}
	var out []*Mutation
	add := func(list []*Mutation) {
		for _, d := range list {
			if !seen[d] {
				seen[d] = true
				out = append(out, d)
			}
		}
	}
	add(m.Parents)
	for _, matched := range m.Matched {
		add(matched)
	}
	slices.SortFunc(out, func(a, b *Mutation) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// UpOrder returns the mutations with every dependency before its dependents, ties broken
// by name. It fails on a cycle, naming the mutations in it. Resolve must have run.
func (r *Revision) UpOrder() ([]*Mutation, error) {
	const (
		visiting = 1
		done     = 2
	)
	state := map[*Mutation]int{}
	var path, order []*Mutation
	var visit func(m *Mutation) error
	visit = func(m *Mutation) error {
		switch state[m] {
		case done:
			return nil
		case visiting:
			names := []string{}
			start := slices.Index(path, m)
			for _, p := range path[start:] {
				names = append(names, p.Name)
			}
			names = append(names, m.Name)
			return oops.In("model").With("namespace", r.Namespace).
				Errorf("namespace %q: dependency cycle: %s", r.Namespace, strings.Join(names, " -> "))
		}
		state[m] = visiting
		path = append(path, m)
		for _, d := range m.Dependencies() {
			if err := visit(d); err != nil {
				return err
			}
		}
		path = path[:len(path)-1]
		state[m] = done
		order = append(order, m)
		return nil
	}
	sorted := slices.Clone(r.Mutations)
	slices.SortFunc(sorted, func(a, b *Mutation) int { return strings.Compare(a.Name, b.Name) })
	for _, m := range sorted {
		if err := visit(m); err != nil {
			return nil, err
		}
	}
	return order, nil
}

// OverrideSet returns a revision made of the override entries that delete: leaves, to test
// what is recorded in their place, or nil when there are none. Dependencies on mutations
// outside the set are dropped, as those are not part of what is tested.
func (r *Revision) OverrideSet() *Revision {
	deleted := map[string]bool{}
	for _, name := range r.Delete {
		deleted[name] = true
	}
	set := &Revision{Namespace: r.Namespace, Number: r.Number, byName: map[string]*Mutation{}}
	for _, o := range r.Override {
		if !deleted[o.Name] {
			cp := *o
			set.Mutations = append(set.Mutations, &cp)
			set.byName[cp.Name] = &cp
		}
	}
	if len(set.Mutations) == 0 {
		return nil
	}
	for _, m := range set.Mutations {
		kept := make([]DepEntry, 0, len(m.Deps))
		for _, entry := range m.Deps {
			if _, ok := set.byName[entry.Pattern]; ok || isPattern(entry.Pattern) {
				kept = append(kept, entry)
			}
		}
		m.Deps = kept
		// The names were checked when the revision loaded, so resolving cannot fail.
		_ = resolveMutation(m, set.byName, set.Mutations, set)
	}
	return set
}

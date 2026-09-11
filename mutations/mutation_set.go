package mutations

import (
	"iter"
	"strings"

	"github.com/samber/oops"
	"github.com/ugurcsen/gods-generic/maps/hashmap"
	"github.com/ugurcsen/gods-generic/sets/hashset"
)

type MutationChildrenMap map[string][]*Mutation

type MutationSet struct {
	*hashmap.Map[string, *Mutation]
	Namespace string
	Revision  int
	File      string

	// Mode, Delete and OverrideMutations are only meaningful on an override-shaped
	// revision (one that declared mode/delete/override/mutations - see readDocument).
	// Mode, when non-empty, is the apply mode ("default"|"full"|"simple") this
	// revision must run under, overriding whatever the CLI invocation was given.
	Mode string
	// Delete is the names of mutations to drop from dmut's tracking table without
	// running a down for them.
	Delete []string
	// OverrideMutations holds the revision's `override:` block: mutations recorded
	// as this revision's new truth for their Core/Glue/Needs/GlueNeeds, but never
	// executed - a separate set, not part of ms's own dependency graph, since these
	// never run for real.
	OverrideMutations *MutationSet
}

// WithOverrides returns a new set where every mutation named in OverrideMutations has
// its Core/Glue/Needs/GlueNeeds replaced by the override's - this is what actually gets
// saved to dmut's tracking table for an override-shaped revision (see RunMutations),
// since override: entries are recorded but never executed. An override entry naming a
// mutation not present in ms at all (an override for something outside mutations:) is
// still included, so a later apply sees it as already recorded. Panics if
// OverrideMutations is nil - callers should check that first.
func (ms *MutationSet) WithOverrides() *MutationSet {
	ms2 := NewMutationSet(ms.Namespace, ms.Revision, ms.File)
	for mut := range ms.AllMutations() {
		if override, ok := ms.OverrideMutations.GetMutation(mut.Name); ok {
			ms2.AddMutation(&Mutation{
				Name:      mut.Name,
				File:      mut.File,
				Namespace: mut.Namespace,
				Needs:     override.Needs,
				Core:      override.Core,
				GlueNeeds: override.GlueNeeds,
				Glue:      override.Glue,
			})
		} else {
			// A shallow copy, not the same pointer: ResolveDependencies below resets
			// and repopulates SqlParents/SqlChildren/etc. in place on every mutation
			// it touches, and ms is a different, still-live MutationSet (RunMutations
			// still uses it, e.g. for the primary test pass) - mutating its own
			// Mutation objects out from under it here would corrupt its graph too.
			cp := *mut
			ms2.AddMutation(&cp)
		}
	}
	for override := range ms.OverrideMutations.AllMutations() {
		if _, ok := ms.GetMutation(override.Name); !ok {
			cp := *override
			ms2.AddMutation(&cp)
		}
	}
	ms2.ResolveDependencies()
	return ms2
}

func (ms *MutationSet) GetMutation(name string) (*Mutation, bool) {
	if ms == nil || ms.Map == nil {
		return nil, false
	}
	if mut, ok := ms.Map.Get(name); ok {
		return mut, true
	}
	return nil, false
}

func (ms *MutationSet) AllMutations() iter.Seq[*Mutation] {
	return func(yield func(*Mutation) bool) {
		if ms == nil || ms.Map == nil {
			return
		}
		for _, mut := range ms.Map.Values() {
			if !yield(mut) {
				return
			}
		}
	}
}

func (mcm MutationChildrenMap) AddChild(parent_name string, child *Mutation) {
	if _, ok := mcm[parent_name]; !ok {
		mcm[parent_name] = []*Mutation{child}
	} else {
		mcm[parent_name] = append(mcm[parent_name], child)
	}
}

func NewMutationSet(namespace string, revision int, file string) *MutationSet {
	return &MutationSet{
		Map:       hashmap.New[string, *Mutation](),
		Namespace: namespace,
		Revision:  revision,
		File:      file,
	}
}

func (ms *MutationSet) HasMutation(name string) bool {
	_, ok := ms.Map.Get(name)
	return ok
}

func (ms *MutationSet) DeleteMutation(mut *Mutation) error {
	if !ms.HasMutation(mut.Name) {
		return oops.In("mutations").With("mutation", mut.Name).Errorf("mutation %s not found", mut.Name)
	}
	ms.Map.Remove(mut.Name)
	return nil
}

func (ms *MutationSet) AddMutation(mut *Mutation) error {
	if existing, ok := ms.GetMutation(mut.Name); ok {
		return oops.In("mutations").
			With("mutation", mut.Name).
			With("file", mut.File).
			With("previous_file", existing.File).
			Errorf("duplicate mutation name %s, defined in both %s and %s", mut.Name, existing.File, mut.File)
	}

	mut.set = ms
	if mut.File == "" {
		mut.File = ms.File
	}
	mut.Namespace = ms.Namespace
	ms.Map.Put(mut.Name, mut)

	return nil
}

func (ms *MutationSet) ResolveDependencies() error {
	for mut := range ms.AllMutations() {
		mut.SqlParents = hashset.New[*Mutation]()
		mut.SqlChildren = hashset.New[*Mutation]()
		mut.MetaParents = hashset.New[*Mutation]()
		mut.MetaChildren = hashset.New[*Mutation]()
	}

	for mut := range ms.AllMutations() {
		// For dotted names, find if there are parents and add them automatically.
		split_name := strings.Split(mut.Name, ".")
		for i := 0; i < len(split_name)-1; i++ {
			parent_name := strings.Join(split_name[:i+1], ".")
			if parent, ok := ms.Map.Get(parent_name); ok {

				mut.Needs = append(mut.Needs, parent.Name)
				mut.GlueNeeds = append(mut.GlueNeeds, parent.Name)
				mut.SqlParents.Add(parent)
				mut.MetaParents.Add(parent)
			}
		}

		for _, parent_name := range mut.Needs {
			if parent, ok := ms.Map.Get(parent_name); !ok {
				return oops.In("mutations").With("mutation", mut.Name).With("dependency", parent_name).Errorf("%s asks for dependency %s which was not found", mut.Name, parent_name)
			} else {
				mut.SqlParents.Add(parent)
				parent.SqlChildren.Add(mut)
			}
		}

		for _, parent_name := range mut.GlueNeeds {
			if parent, ok := ms.Map.Get(parent_name); !ok {
				return oops.In("mutations").With("mutation", mut.Name).With("meta_dependency", parent_name).Errorf("%s asks for meta dependency %s which was not found", mut.Name, parent_name)
			} else {
				mut.MetaParents.Add(parent)
				parent.MetaChildren.Add(mut)
			}
		}
	}

	for mut := range ms.AllMutations() {
		// Standard DFS cycle detection: `on_stack` is the current recursion path, so
		// finding a dependency already on it is a cycle and the path is exactly that
		// stack. `cleared` is nodes already proven cycle-free from this starting point,
		// so we don't re-walk them.
		on_stack := hashset.New[*Mutation]()
		cleared := hashset.New[*Mutation]()
		var stack []string

		var iterate func(inner_mut *Mutation) []string
		iterate = func(inner_mut *Mutation) []string {
			stack = append(stack, inner_mut.Name)
			on_stack.Add(inner_mut)

			for _, deps := range []*hashset.Set[*Mutation]{inner_mut.SqlParents, inner_mut.MetaParents} {
				for _, dep := range deps.Values() {
					if dep == mut {
						return append(append([]string{}, stack...), mut.Name)
					}
					if cleared.Contains(dep) || on_stack.Contains(dep) {
						continue
					}
					if path := iterate(dep); path != nil {
						return path
					}
				}
			}

			stack = stack[:len(stack)-1]
			on_stack.Remove(inner_mut)
			cleared.Add(inner_mut)
			return nil
		}

		if cycle := iterate(mut); cycle != nil {
			return oops.In("mutations").With("cycle", cycle).Errorf("%s causes a dependency cycle", mut.Name)
		}
	}

	return nil
}

// Compares both mutationsets
func (ms *MutationSet) GetMutationsDelta(other *MutationSet, dir IterationDirection) (
	to_down *RunnableMap,
	to_up *RunnableMap,
) {
	to_down = NewRunnableMap()
	to_up = NewRunnableMap()

	// Start by computing obsoletes
	for mut := range other.AllMutations() {
		// A mutation is obsolete if it is not in the local set or if its hash is different. In both cases, it must be downed.

		if _, ok := to_down.Get(mut.Name); ok {
			// We already have seen it.
			continue
		}

		var down_dir = IterationDirection{Down: true, Meta: dir.Meta}
		if local, ok := ms.GetMutation(mut.Name); !ok || local.Hash(dir) != mut.Hash(dir) {
			for dep := range mut.IterateDependencies(down_dir) {
				to_down.Put(dep.Name, dep.Runnable(down_dir))
			}
		}
	}

	var up_dir = IterationDirection{Down: false, Meta: dir.Meta}
	for mut := range ms.AllMutations() {
		for dep := range mut.IterateDependencies(up_dir) {
			_, was_downed := to_down.Get(dep.Name)
			_, in_other := other.GetMutation(dep.Name)

			if !in_other || was_downed {
				to_up.Put(dep.Name, dep.Runnable(up_dir))
			}
		}
	}

	return to_down, to_up
}

// GetLCPDelta computes down/up runnables for one direction (core when !dir.Meta, glue when
// dir.Meta) using a longest-common-prefix diff of each mutation's own statement array,
// instead of treating any change as reason to down/up the whole mutation - see
// specs/changes.md, "New algorithm".
//
// A mutation absent from the local set entirely is still fully obsolete: it, and everything
// depending on it, is downed in full - unrelated to the LCP tail logic, since there's no
// incoming version to reconcile against. Likewise, a mutation forced down as collateral of a
// vanished dependency is downed and re-upped in full, not by its own tail, since nothing
// partial survives a full teardown of something it depended on.
//
// any_real_down reports whether any mutation needed a non-empty old tail (as opposed to a
// pure append or a brand-new mutation) - callers use this to decide whether glue needs the
// old whole-mutation teardown instead of this same LCP treatment (see RunMutations).
func (ms *MutationSet) GetLCPDelta(other *MutationSet, dir IterationDirection) (
	to_down *RunnableMap,
	to_up *RunnableMap,
	any_real_down bool,
) {
	to_down = NewRunnableMap()
	to_up = NewRunnableMap()

	var down_dir = IterationDirection{Down: true, Meta: dir.Meta}
	var up_dir = IterationDirection{Down: false, Meta: dir.Meta}

	statementsOf := func(mut *Mutation) []MutationStatement {
		if mut == nil {
			return nil
		}
		if dir.Meta {
			return mut.Glue
		}
		return mut.Core
	}
	withStatements := func(mut *Mutation, stmts []MutationStatement) *Mutation {
		cp := *mut
		if dir.Meta {
			cp.Glue = stmts
		} else {
			cp.Core = stmts
		}
		return &cp
	}

	cascaded := hashset.New[string]()

	// Obsolete mutations (gone entirely from the local set) still down in full,
	// cascading to everything that depends on them - unrelated to the LCP tail logic
	// below, since there's no incoming version to reconcile against. IterateDependencies
	// yields dependents before the mutation itself, which is already correct leaves-
	// first order.
	for mut := range other.AllMutations() {
		if _, ok := ms.GetMutation(mut.Name); ok {
			continue
		}
		for dep := range mut.IterateDependencies(down_dir) {
			to_down.Put(dep.Name, dep.Runnable(down_dir))
			cascaded.Add(dep.Name)
		}
	}

	// Diff every mutation present on both sides (and not already claimed by a cascade
	// above) by longest common prefix - only statements after the first divergence
	// need downing (from what's recorded) or upping (from what's incoming). This pass
	// only computes *what* each mutation needs; insertion into to_down/to_up (which
	// determines execution order) happens below, in dependency order rather than
	// ms.AllMutations()'s unspecified map order - a schema must always come up before
	// what's declared in it, and go down only after.
	type tail struct {
		old, new         []MutationStatement
		other_mut, local *Mutation
	}
	tails := make(map[string]tail)
	for local := range ms.AllMutations() {
		if cascaded.Contains(local.Name) {
			continue
		}
		other_mut, _ := other.GetMutation(local.Name)

		old_list := statementsOf(other_mut)
		new_list := statementsOf(local)
		prefix := commonPrefixLen(old_list, new_list)
		old_tail := old_list[prefix:]
		new_tail := new_list[prefix:]
		if len(old_tail) == 0 && len(new_tail) == 0 {
			continue
		}
		if len(old_tail) > 0 {
			any_real_down = true
		}
		tails[local.Name] = tail{old_tail, new_tail, other_mut, local}
	}

	// IterateDependencies(up_dir) yields ancestors before self; IterateDependencies
	// (down_dir) yields dependents before self. Walking every local mutation's own
	// chain guarantees the right relative order regardless of which mutation the
	// outer loop happens to reach first - by the time any mutation is inserted, every
	// mutation before it in the chain already was, on an earlier iteration if not this
	// one.
	for local := range ms.AllMutations() {
		for dep := range local.IterateDependencies(up_dir) {
			if _, done := to_up.Get(dep.Name); done {
				continue
			}
			if t, ok := tails[dep.Name]; ok && len(t.new) > 0 {
				to_up.Put(dep.Name, withStatements(t.local, t.new).Runnable(up_dir))
			} else if cascaded.Contains(dep.Name) && len(statementsOf(dep)) > 0 {
				// Downed in full as collateral above: needs a full rebuild, not a tail.
				to_up.Put(dep.Name, dep.Runnable(up_dir))
			}
		}
		for dep := range local.IterateDependencies(down_dir) {
			if _, done := to_down.Get(dep.Name); done {
				continue
			}
			if t, ok := tails[dep.Name]; ok && len(t.old) > 0 {
				to_down.Put(dep.Name, withStatements(t.other_mut, t.old).Runnable(down_dir))
			}
		}
	}

	return to_down, to_up, any_real_down
}

// ExpandForFullRebuild builds a down/up pair that fully rebuilds every named mutation and
// everything depending on it, leaves first - used by `full` apply mode when a tail-only
// GetLCPDelta attempt fails: names is every mutation whose down was attempted (see
// RunMutations), and this reruns them (and their dependents) in full rather than by tail,
// exactly like the cascade branch in GetLCPDelta for an obsolete mutation, just seeded from
// "this attempt failed" instead of "this mutation vanished."
func (ms *MutationSet) ExpandForFullRebuild(names []string, other *MutationSet, dir IterationDirection) (
	to_down *RunnableMap,
	to_up *RunnableMap,
) {
	to_down = NewRunnableMap()
	to_up = NewRunnableMap()

	var down_dir = IterationDirection{Down: true, Meta: dir.Meta}
	var up_dir = IterationDirection{Down: false, Meta: dir.Meta}

	// Collect every affected mutation (each seed plus its dependents, recursively) in
	// down order - dependents before the thing they depend on - using the recorded/
	// distant graph, since that's what's actually being torn down.
	var affected []string
	seen := hashset.New[string]()
	for _, name := range names {
		other_mut, ok := other.GetMutation(name)
		if !ok {
			continue
		}
		for dep := range other_mut.IterateDependencies(down_dir) {
			to_down.Put(dep.Name, dep.Runnable(down_dir))
			if !seen.Contains(dep.Name) {
				seen.Add(dep.Name)
				affected = append(affected, dep.Name)
			}
		}
	}

	// Rebuild in the reverse of down order: a dependents-before-self sequence,
	// reversed, is exactly the self-before-dependents order rebuilding needs (a table
	// must exist before another table's foreign key can reference it, etc). Mutations
	// no longer present locally are simply gone, not rebuilt.
	for i := len(affected) - 1; i >= 0; i-- {
		name := affected[i]
		if local_mut, ok := ms.GetMutation(name); ok {
			to_up.Put(name, local_mut.Runnable(up_dir))
		}
	}

	return to_down, to_up
}

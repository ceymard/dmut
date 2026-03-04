package mutations

import (
	"fmt"
	"iter"
	"strings"

	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
	"github.com/ugurcsen/gods-generic/maps/hashmap"
	"github.com/ugurcsen/gods-generic/maps/linkedhashmap"
	"github.com/ugurcsen/gods-generic/sets/hashset"
)

type MutationChildrenMap map[string][]*Mutation

type MutationSet struct {
	*hashmap.Map[string, *Mutation]
	Namespace string
	Revision  int
	File      string

	Roles *hashset.Set[string]
}

func (ms *MutationSet) GetMutation(name string) (*Mutation, bool) {
	if mut, ok := ms.Map.Get(name); ok {
		return mut, true
	}
	return nil, false
}

func (ms *MutationSet) AllMutations() iter.Seq[*Mutation] {
	return func(yield func(*Mutation) bool) {
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
		Roles:     hashset.New[string](),
	}
}

func (ms *MutationSet) HasMutation(name string) bool {
	_, ok := ms.Map.Get(name)
	return ok
}

func (ms *MutationSet) DeleteMutation(mut *Mutation) error {
	if !ms.HasMutation(mut.Name) {
		return fmt.Errorf("mutation %s not found", mut.Name)
	}
	ms.Map.Remove(mut.Name)
	return nil
}

func (ms *MutationSet) AddMutation(mut *Mutation) error {
	if ms.HasMutation(mut.Name) {
		return fmt.Errorf("duplicate migration name: %s", mut.Name)
	}

	mut.set = ms
	if mut.File == "" {
		mut.File = ms.File
	}
	mut.Namespace = ms.Namespace
	ms.Map.Put(mut.Name, mut)
	mut.Normalize()

	return nil
}

func (ms *MutationSet) UnmarshalYAML(node ast.Node) error {

	map_node, ok := node.(*ast.MappingNode)
	if !ok {
		return fmt.Errorf("expected mapping node, got %T", node)
	}

	for _, value := range map_node.Values {
		if value.Key.String() == "__namespace" {
			yaml.NodeToValue(value.Value, &ms.Namespace)
		} else if value.Key.String() == "__revision" {
			yaml.NodeToValue(value.Value, &ms.Revision)
		} else {
			var mutation_name string

			var mutation = &Mutation{
				Name: mutation_name,
				File: ms.File,
			}
			yaml.NodeToValue(value.Key, &mutation_name)
			yaml.NodeToValue(value.Value, mutation)
		}
	}
	return nil
}

func (ms *MutationSet) ResolveDependencies() error {
	// FIXME we should test for cycles
	for mut := range ms.AllMutations() {
		// For dotted names, find if there are parents and add them automatically.
		split_name := strings.Split(mut.Name, ".")
		for i := 0; i < len(split_name)-1; i++ {
			parent_name := strings.Join(split_name[:i+1], ".")
			if parent, ok := ms.Map.Get(parent_name); ok {
				mut.Needs = append(mut.Needs, parent.Name)
				mut.MetaNeeds = append(mut.MetaNeeds, parent.Name)
				mut.SqlParents.Add(parent)
				mut.MetaParents.Add(parent)
			}
		}

		for _, parent_name := range mut.Needs {
			if parent, ok := ms.Map.Get(parent_name); !ok {
				return fmt.Errorf("%s asks for dependency %s which was not found", mut.Name, parent_name)
			} else {
				mut.SqlParents.Add(parent)
				parent.SqlChildren.Add(mut)
			}
		}

		for _, parent_name := range mut.MetaNeeds {
			if parent, ok := ms.Map.Get(parent_name); !ok {
				return fmt.Errorf("%s asks for meta dependency %s which was not found", mut.Name, parent_name)
			} else {
				mut.MetaParents.Add(parent)
				parent.MetaChildren.Add(mut)
			}
		}

	}
	return nil
}

// Compares both mutationsets
func (ms *MutationSet) GetMutationsDelta(other *MutationSet, dir IterationDirection) (
	to_down *linkedhashmap.Map[string, *Runnable],
	to_up *linkedhashmap.Map[string, *Runnable],
) {
	to_down = linkedhashmap.New[string, *Runnable]()
	to_up = linkedhashmap.New[string, *Runnable]()

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

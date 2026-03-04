package mutations

import (
	"fmt"
	"slices"

	"github.com/samber/lo"
)

type RevisionSequence []*MutationSet

type MutationNamespace map[string]RevisionSequence

// Ensure there is no gap in the revision sequence and that there is a default revision that will be applied
func (ns MutationNamespace) EnsureContinuousRevisions() error {
	for namespace_name, namespace := range ns {

		// Sort by revision order in ascending order, with the exception of 0 being the highest
		slices.SortFunc(namespace, func(a, b *MutationSet) int {
			if a.Revision == 0 {
				return 1
			}
			if b.Revision == 0 {
				return -1
			}
			return a.Revision - b.Revision
		})

		has_zero := false
		for i := 1; i < len(namespace)-1; i++ {
			if namespace[i].Revision == 0 {
				has_zero = true
				continue
			}
			if namespace[i].Revision != namespace[i-1].Revision+1 {
				return fmt.Errorf("revision %d is not continuous", namespace[i].Revision)
			}
		}

		if !has_zero {
			return fmt.Errorf("there is not default mutation set for namespace %s", namespace_name)
		}
	}
	return nil
}

func (ns MutationNamespace) AddSet(set *MutationSet) error {
	if namespace, ok := ns[set.Namespace]; ok {
		// Try to find a set with the same revision
		if revisionSet, ok := lo.Find(namespace, func(s *MutationSet) bool {
			return s.Revision == set.Revision
		}); ok {
			// Merge the sets
			for mut := range set.AllMutations() {
				if err := revisionSet.AddMutation(mut); err != nil {
					return err
				}
			}
		} else {
			// Append the set to the namespace
			ns[set.Namespace] = append(namespace, set)
		}

	} else {
		ns[set.Namespace] = []*MutationSet{set}
	}

	return nil
}

func (ns MutationNamespace) ResolveDependencies() error {
	for _, namespace := range ns {
		for _, set := range namespace {
			if err := set.ResolveDependencies(); err != nil {
				return err
			}
		}
	}
	return nil
}

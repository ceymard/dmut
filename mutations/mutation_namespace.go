package mutations

import (
	"fmt"

	"github.com/ugurcsen/gods-generic/maps/linkedhashmap"
)

type RevisionSequence struct {
	Revisions   map[int]*MutationSet
	MaxRevision int
}

func NewRevisionSequence() *RevisionSequence {
	return &RevisionSequence{
		Revisions:   make(map[int]*MutationSet),
		MaxRevision: 0,
	}
}

func (rs *RevisionSequence) AddSet(set *MutationSet) error {

	// Try to find a set with the same revision
	if revisionSet, ok := rs.Revisions[set.Revision]; ok {
		// Merge the sets
		for mut := range set.AllMutations() {
			if err := revisionSet.AddMutation(mut); err != nil {
				return err
			}
		}
	} else {
		rs.Revisions[set.Revision] = set
	}

	if set.Revision > rs.MaxRevision {
		rs.MaxRevision = set.Revision
	}

	return nil
}

type MutationNamespace struct {
	*linkedhashmap.Map[string, *RevisionSequence]
}

func NewMutationNamespace() *MutationNamespace {
	return &MutationNamespace{
		Map: linkedhashmap.New[string, *RevisionSequence](),
	}
}

// Ensure there is no gap in the revision sequence and that there is a default revision that will be applied
func (ns MutationNamespace) EnsureContinuousRevisions() error {
	for namespace_name, revision_sequence := range ns.Values() {

		if len(revision_sequence.Revisions) == 0 || revision_sequence.Revisions[0].Revision != 0 {
			return fmt.Errorf("there is not default mutation set for namespace %s", namespace_name)
		}

		for i := 1; i < len(revision_sequence.Revisions)-1; i++ {
			if revision_sequence.Revisions[i].Revision != revision_sequence.Revisions[i-1].Revision+1 {
				return fmt.Errorf("revision %d is not continuous", revision_sequence.Revisions[i].Revision)
			}
		}

	}
	return nil
}

func (ns MutationNamespace) AddSet(set *MutationSet) error {
	if revision_sequence, ok := ns.Map.Get(set.Namespace); ok {
		revision_sequence.AddSet(set)
	}

	return nil
}

func (ns MutationNamespace) ResolveDependencies() error {
	for _, namespace := range ns.Values() {
		for _, set := range namespace.Revisions {
			if err := set.ResolveDependencies(); err != nil {
				return err
			}
		}
	}
	return nil
}

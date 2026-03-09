package mutations

import (
	"math"

	"github.com/samber/oops"
	"github.com/ugurcsen/gods-generic/maps/linkedhashmap"
)

type RevisionSequence struct {
	Revisions   map[int]*MutationSet
	MaxRevision int
	MinRevision int
}

func NewRevisionSequence() *RevisionSequence {
	return &RevisionSequence{
		Revisions:   make(map[int]*MutationSet),
		MaxRevision: 0,
		MinRevision: math.MaxInt,
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

	if set.Revision < rs.MinRevision && set.Revision != 0 {
		rs.MinRevision = set.Revision
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
	for _, namespace := range ns.Keys() {
		revision_sequence, ok := ns.Map.Get(namespace)
		if !ok {
			return oops.In("mutations").With("namespace", namespace).Errorf("no revision sequence found")
		}

		if revision, ok := revision_sequence.Revisions[0]; ok {
			revision.Revision = revision_sequence.MaxRevision + 1
			revision_sequence.MaxRevision = revision.Revision
			delete(revision_sequence.Revisions, 0)
			revision_sequence.AddSet(revision)
		}

		if len(revision_sequence.Revisions) == 0 {
			return oops.In("mutations").With("namespace", namespace).Errorf("no revisions found")
		}

		if len(revision_sequence.Revisions) != revision_sequence.MaxRevision-revision_sequence.MinRevision+1 {
			return oops.In("mutations").With("namespace", namespace).With("min_revision", revision_sequence.MinRevision).With("max_revision", revision_sequence.MaxRevision).Errorf("revision sequence is not continuous")
		}
	}
	return nil
}

func (ns MutationNamespace) AddSet(set *MutationSet) error {
	if revision_sequence, ok := ns.Map.Get(set.Namespace); ok {
		revision_sequence.AddSet(set)
	} else {
		revision_sequence = NewRevisionSequence()
		ns.Map.Put(set.Namespace, revision_sequence)
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

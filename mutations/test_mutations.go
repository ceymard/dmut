package mutations

import (
	"log"
	"slices"
)

// Testing happens in two phases:
// First, all leaf mutations are tested independently.
// Secondly, all mutations are passed, and then all are removed one by one with their dependencies.
// The tests are only done on the current batch of mutations.

func TestMutationsIndependently(runner Runner, mutations DbMutationMap) error {

	if err := runner.SavePoint("test_mutations_independently"); err != nil {
		return err
	}
	defer runner.RollbackToSavepoint("test_mutations_independently")

	if err := runner.SavePoint("test_leaf_mutation"); err != nil {
		return err
	}
	// Test all leaf mutations independently
	for _, leaf := range mutations.GetLeafMutations() {
		log.Println("testing leaf mutation", leaf.DisplayName())
		mut_slice := mutations.GetMutationsInOrder(true, leaf.Hash)
		for _, mut := range mut_slice {
			if err := runner.ApplyMutation(mut); err != nil {
				return err
			}
		}

		slices.Reverse(mut_slice)
		for _, m := range mut_slice {
			if err := runner.UndoMutation(m); err != nil {
				return err
			}
		}

		if err := runner.RollbackToSavepoint("test_leaf_mutation"); err != nil {
			return err
		}
	}

	// Now do all the mutations, as we will then down them from the full set.
	for _, mut := range mutations.GetMutationsInOrder(true) {
		if err := runner.ApplyMutation(mut); err != nil {
			return err
		}
	}

	if err := runner.SavePoint("test_mutation_full"); err != nil {
		return err
	}
	for _, mut := range mutations {
		// For this mutation, we get all its children in undo order, and undo them one by one
		for _, m := range mutations.GetMutationsInOrder(false, mut.Hash) {
			if err := runner.UndoMutation(m); err != nil {
				return err
			}
		}

		if err := runner.RollbackToSavepoint("test_mutation_full"); err != nil {
			return err
		}
	}

	return nil
}

func TestMutationsFromOriginal(runner Runner, test_runner Runner, mutations DbMutationMap) error {

	current, err := runner.GetDBMutationsFromDb()
	if err != nil {
		return err
	}

	if err := test_runner.SavePoint("test_mutations_from_original"); err != nil {
		return err
	}

	log.Println("applying mutations of database to test database")
	for _, mut := range current.GetMutationsInOrder(true) {
		if err := test_runner.ApplyMutation(mut); err != nil {
			return err
		}
	}

	log.Println("now run the mutations from disk")
	// Test applying the mutations
	if err := ApplyMutationsFromCurrent(test_runner, mutations); err != nil {
		return err
	}

	if err := test_runner.RollbackToSavepoint("test_mutations_from_original"); err != nil {
		return err
	}

	if err := TestMutationsIndependently(test_runner, mutations); err != nil {
		return err
	}

	return nil
}

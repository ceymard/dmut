package mutations

import (
	"slices"
)

// Test all leaf mutations, ideally in a blank database.
func TestLeafMutations(runner Runner, mutations DbMutationMap) error {

	if err := runner.SavePoint("test_leaf_mutation"); err != nil {
		return err
	}
	// Test all leaf mutations independently
	for _, mutation := range mutations {
		runner.Logger().Println("testing mutation", mutation.DisplayName())
		mut_slice := mutations.GetMutationsInOrder(true, mutation.Hash)
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

	return nil
}

// Import the mutations in database into a test database.
func TestImportLoadedMutations(test_runner Runner, runner Runner) error {
	current, err := runner.GetDBMutationsFromDb()
	if err != nil {
		return err
	}

	for _, mut := range current.GetMutationsInOrder(true) {
		if err := test_runner.ApplyMutation(mut); err != nil {
			return err
		}
	}

	return nil
}

// Test downing all mutations one by one. Assume that all mutations have been applied beforehand.
func TestDowningMutations(runner Runner) error {
	if err := runner.SavePoint("test_mutation_full"); err != nil {
		return err
	}

	mutations, err := runner.GetDBMutationsFromDb()
	if err != nil {
		return err
	}

	for _, mut := range mutations {
		// For this mutation, we get all its children in undo order, and undo them one by one
		runner.Logger().Println("testing downing of", mut.DisplayName())
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

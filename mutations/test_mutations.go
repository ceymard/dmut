package mutations

// Testing happens in two phases:
// First, all leaf mutations are tested independently.
// Secondly, all mutations are passed, and then all are removed one by one with their dependencies.
// The tests are only done on the current batch of mutations.

func testMutations(runner Runner, mutations DbMutationMap) error {

	runner.SavePoint("test_leaf_mutation")
	// Test all leaf mutations independently
	for _, leaf := range mutations.GetLeafMutations() {

		for _, mut := range mutations.GetMutationsInOrder(true, leaf.Hash) {
			if err := runner.ApplyMutation(mut); err != nil {
				return err
			}
		}

		for _, m := range mutations.GetMutationsInOrder(false, leaf.Hash) {
			if err := runner.UndoMutation(m); err != nil {
				return err
			}
		}

		runner.RollbackToSavepoint("test_leaf_mutation")
	}

	// Now do all the mutations, as we will then down them from the full set.
	for _, mut := range mutations.GetMutationsInOrder(true) {
		if err := runner.ApplyMutation(mut); err != nil {
			return err
		}
	}

	runner.SavePoint("test_mutation_full")
	for _, mut := range mutations {
		// For this mutation, we get all its children in undo order, and undo them one by one
		for _, m := range mutations.GetMutationsInOrder(false, mut.Hash) {
			if err := runner.UndoMutation(m); err != nil {
				return err
			}
		}

		runner.RollbackToSavepoint("test_mutation_full")
	}

	return nil
}

func TestMutationsFromOriginal(runner Runner, test_runner Runner, mutations DbMutationMap) error {
	current, err := runner.GetDBMutationsFromDb()
	if err != nil {
		return err
	}

	test_runner.SavePoint("test_mutations_from_original")
	if err := ApplyMutationsFromCurrent(test_runner, mutations, current); err != nil {
		return err
	}
	test_runner.RollbackToSavepoint("test_mutations_from_original")

	return TestMutations(test_runner, mutations)
}

func TestMutations(runner Runner, mutations DbMutationMap) error {
	runner.SavePoint("test_mutations")
	defer runner.RollbackToSavepoint("test_mutations")

	return testMutations(runner, mutations)
}

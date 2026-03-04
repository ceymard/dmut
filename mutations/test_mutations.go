package mutations

func TestMutationsInTestDatabase(test_runner Executor, set *MutationSet) error {
	if err := test_runner.SavePoint("test_mutations"); err != nil {
		return err
	}

	if err := MutationTestSequence(test_runner, set, ITER_SQL); err != nil {
		return err
	}

	// Apply the SQL
	up_sql, _ := set.GetMutationsDelta(nil, ITER_SQL_UP)
	for _, up_runnable := range up_sql.Values() {
		if err := test_runner.Run(up_runnable); err != nil {
			return err
		}
	}

	// Try the meta
	if err := MutationTestSequence(test_runner, set, ITER_META); err != nil {
		return err
	}

	// Undo it all
	if err := test_runner.RollbackToSavepoint("test_mutations"); err != nil {
		return err
	}

	return nil

}

// With the test runner, try to up all mutations independently, and reset after each one.
func MutationTestSequence(runner Executor, set *MutationSet, dir IterationDirection) error {

	if err := runner.SavePoint("independent_test"); err != nil {
		return err
	}

	// Test all mutations independently
	for mutation := range set.AllMutations() {

		if err := mutation.RunRecursively(runner, ITER_SQL_UP); err != nil {
			return err
		}

		// And then down them
		if err := mutation.RunRecursively(runner, ITER_SQL_DOWN); err != nil {
			return err
		}

		if err := runner.RollbackToSavepoint("independent_test"); err != nil {
			return err
		}
	}

	return nil
}

// Test downing all mutations in the set independently. Consider they have been applied beforehand.
func TestDowningMutations(runner Executor, set *MutationSet) error {
	if err := runner.SavePoint("test_mutation_full"); err != nil {
		return err
	}

	for mut := range set.AllMutations() {

		// For this mutation, we get all its children in undo order, and undo them one by one
		runner.Logger().Println("testing downing of", mut.DisplayName())
		if err := mut.RunRecursively(runner, ITER_META_DOWN); err != nil {
			return err
		}

		if err := runner.RollbackToSavepoint("test_mutation_full"); err != nil {
			return err
		}
	}

	if err := runner.SavePoint("test_mutation_after_metas_removed"); err != nil {
		return err
	}

	// Remove the metas
	_, down_meta := set.GetMutationsDelta(nil, ITER_META)
	for _, down_meta := range down_meta.Values() {
		if err := runner.Run(down_meta); err != nil {
			return err
		}
	}

	for mut := range set.AllMutations() {
		if err := mut.RunRecursively(runner, ITER_SQL_DOWN); err != nil {
			return err
		}

		if err := runner.RollbackToSavepoint("test_mutation_after_metas_removed"); err != nil {
			return err
		}
	}

	// Undo all our removals
	if err := runner.RollbackToSavepoint("test_mutation_full"); err != nil {
		return err
	}

	return nil
}

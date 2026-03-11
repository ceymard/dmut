package mutations

import (
	"slices"

	au "github.com/logrusorgru/aurora"
)

// Test a mutation set by running all mutations independently, and resetting after each one.
// Consider that the set is already up in the database.
func TestMutationSet(runner Executor, set *MutationSet) (err error) {

	runner.SetTesting()
	defer func() {
		runner.ResumeLogging()
		if err != nil {
			runner.Logger().Println(runner.GetStringOutput())
		}
	}()

	if err = runner.SavePoint("test_mutation_set"); err != nil {
		return err
	}

	// Downing
	var fake_empty_local_set *MutationSet = nil
	sql_down, _ := fake_empty_local_set.GetMutationsDelta(set, ITER_SQL)
	meta_down, _ := fake_empty_local_set.GetMutationsDelta(set, ITER_META)

	// Down the meta to be able to test it independently
	if err = meta_down.Run(runner); err != nil {
		return err
	}

	// Test the meta
	if err = MutationTestSequence(runner, set, ITER_META); err != nil {
		return err
	}

	runner.Logger().Println("Downing SQL", sql_down.Size())

	// Then, down the SQL
	if err = sql_down.Run(runner); err != nil {
		return err
	}

	// Test the SQL
	if err = MutationTestSequence(runner, set, ITER_SQL); err != nil {
		return err
	}

	if err = runner.RollbackToSavepoint("test_mutation_set"); err != nil {
		runner.Logger().Println(au.BrightRed("error rollbacking to savepoint"), err)
	}

	if err = runner.ReleaseSavepoint("test_mutation_set"); err != nil {
		runner.Logger().Println(au.BrightRed("error rollbacking to savepoint"), err)
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
		runner.Logger().Printf("testing %s.%s\n", mutation.DisplayName(), dir.MetaOrSql())

		var inner_mutations []*Mutation
		for mut := range mutation.IterateDependencies(dir) {
			inner_mutations = append(inner_mutations, mut)
		}

		for _, mut := range inner_mutations {
			if err := runner.Run(mut.Runnable(dir)); err != nil {
				return err
			}
		}

		down_dir := dir
		down_dir.Down = true
		slices.Reverse(inner_mutations)
		for _, mut := range inner_mutations {
			if err := runner.Run(mut.Runnable(down_dir)); err != nil {
				return err
			}
		}

		if err := runner.RollbackToSavepoint("independent_test"); err != nil {
			return err
		}
	}

	if err := runner.ReleaseSavepoint("independent_test"); err != nil {
		return err
	}

	return nil
}

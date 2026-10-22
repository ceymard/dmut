package mutations

import (
	"context"
	"slices"
)

// Test a mutation set by running all mutations independently, and resetting after each one.
// Consider that the set is already up in the database.
func TestMutationSet(ctx context.Context, runner Executor, set *MutationSet) (err error) {

	runner.BeginCapture()
	defer func() { runner.EndCapture(err != nil) }()

	if err = runner.SavePoint(ctx, "test_mutation_set"); err != nil {
		return err
	}

	// Downing
	var fake_empty_local_set *MutationSet = nil
	sql_down, _ := fake_empty_local_set.GetMutationsDelta(set, ITER_SQL)
	meta_down, _ := fake_empty_local_set.GetMutationsDelta(set, ITER_META)

	// Down the meta to be able to test it independently
	if err = meta_down.Run(ctx, runner); err != nil {
		return err
	}

	// Test the meta
	if err = MutationTestSequence(ctx, runner, set, ITER_META); err != nil {
		return err
	}

	runner.Logger().Info("downing sql", "count", sql_down.Size())

	// Then, down the SQL
	if err = sql_down.Run(ctx, runner); err != nil {
		return err
	}

	// Test the SQL
	if err = MutationTestSequence(ctx, runner, set, ITER_SQL); err != nil {
		return err
	}

	if err = runner.RollbackToSavepoint(ctx, "test_mutation_set"); err != nil {
		runner.Logger().Error("error rolling back to savepoint", "error", err)
	}

	if err = runner.ReleaseSavepoint(ctx, "test_mutation_set"); err != nil {
		runner.Logger().Error("error releasing savepoint", "error", err)
	}

	return nil

}

// With the test runner, try to up all mutations independently, and reset after each one.
func MutationTestSequence(ctx context.Context, runner Executor, set *MutationSet, dir IterationDirection) error {

	if err := runner.SavePoint(ctx, "independent_test"); err != nil {
		return err
	}

	// Test all mutations independently
	for mutation := range set.AllMutations() {
		runner.Logger().Info("testing", append(mutation.LogAttrs(), "kind", dir.MetaOrSql())...)

		var inner_mutations []*Mutation
		for mut := range mutation.IterateDependencies(dir) {
			inner_mutations = append(inner_mutations, mut)
		}

		for _, mut := range inner_mutations {
			if err := runner.Run(ctx, mut.Runnable(dir)); err != nil {
				return err
			}
		}

		down_dir := dir
		down_dir.Down = true
		slices.Reverse(inner_mutations)
		for _, mut := range inner_mutations {
			if err := runner.Run(ctx, mut.Runnable(down_dir)); err != nil {
				return err
			}
		}

		if err := runner.RollbackToSavepoint(ctx, "independent_test"); err != nil {
			return err
		}
	}

	if err := runner.ReleaseSavepoint(ctx, "independent_test"); err != nil {
		return err
	}

	return nil
}

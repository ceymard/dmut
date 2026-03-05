package mutations

import (
	"slices"

	au "github.com/logrusorgru/aurora"
)

func TestAllMutations(runner Executor, namespaces *MutationNamespace) (err error) {

	// Create the test runner before BEGIN, so that we have the test database ready before modifying the roles.

	if err = runner.Begin(); err != nil {
		return err
	}
	rollbacked := false
	defer func() {
		if !rollbacked {
			if err2 := runner.Rollback(); err2 != nil {
				runner.Logger().Println(au.BrightRed("error rolling back test database"), err2)
			}
		}
	}()

	if err = runner.SavePoint("test_namespaces"); err != nil {
		return err
	}

	for _, namespace := range namespaces.Values() {

		namespace_name := namespace.Revisions[0].Namespace
		db_mutations, err := runner.GetDBMutationsFromDb(namespace_name)
		if err != nil {
			return err
		}

		// Start by downing all mutations in the database
		runner.Logger().Println(au.Bold(au.BrightGreen("downing all mutations in the database")).String())
		var empty_set *MutationSet = NewMutationSet(namespace_name, 0, "")

		meta_down, _ := empty_set.GetMutationsDelta(db_mutations, ITER_META)
		if err = meta_down.Run(runner); err != nil {
			return err
		}

		sql_down, _ := empty_set.GetMutationsDelta(db_mutations, ITER_SQL)
		if err = sql_down.Run(runner); err != nil {
			return err
		}

		if err = runner.SavePoint("test_mutations"); err != nil {
			return err
		}

		for _, revision := range namespace.Revisions {

			if err = TestMutationSet(runner, revision); err != nil {
				return err
			}

			if err = runner.RollbackToSavepoint("test_mutations"); err != nil {
				return err
			}
		}

		if err := runner.RollbackToSavepoint("test_namespaces"); err != nil {
			return err
		}
	}

	if err = runner.Rollback(); err != nil {
		return err
	}
	rollbacked = true

	return nil
}

func TestMutationSet(test_runner Executor, set *MutationSet) error {

	if err := MutationTestSequence(test_runner, set, ITER_SQL); err != nil {
		return err
	}

	// Apply the SQL
	test_runner.Logger().Println(au.Bold(au.BrightGreen("applying SQL")).String())
	_, up_sql := set.GetMutationsDelta(nil, ITER_SQL_UP)
	for _, up_runnable := range up_sql.Values() {
		if err := test_runner.Run(up_runnable); err != nil {
			return err
		}
	}

	// Try the meta
	if err := MutationTestSequence(test_runner, set, ITER_META); err != nil {
		return err
	}

	test_runner.Logger().Println(au.Bold(au.BrightGreen("Applying meta")).String())
	_, up_meta := set.GetMutationsDelta(nil, ITER_META_UP)
	for _, up_runnable := range up_meta.Values() {
		if err := test_runner.Run(up_runnable); err != nil {
			return err
		}
	}

	return nil

}

// With the test runner, try to up all mutations independently, and reset after each one.
func MutationTestSequence(runner Executor, set *MutationSet, dir IterationDirection) error {

	if err := runner.SavePoint("independent_test"); err != nil {
		return err
	}
	defer runner.ReleaseSavepoint("independent_test")

	// Test all mutations independently
	for mutation := range set.AllMutations() {
		runner.Logger().Println("testing", mutation.DisplayName(), dir.String())

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

	return nil
}

package mutations

import (
	"fmt"
	"os"
	"slices"

	"github.com/jackc/pgx/v4"
	au "github.com/logrusorgru/aurora"
)

func TestAllMutationsInTestDatabase(runner Executor, namespaces *MutationNamespace) (err error) {

	// Create the test runner before BEGIN, so that we have the test database ready before modifying the roles.
	var test_runner Executor
	test_runner, err = runner.GetTestExecutor()
	if err != nil {
		return err
	}
	defer test_runner.Close()
	defer func() {
		if err != nil {
			os.Stdout.WriteString(test_runner.GetTestOutput())
		}
	}()

	if err = test_runner.Begin(); err != nil {
		return err
	}
	rollbacked := false
	defer func() {
		if !rollbacked {
			if err2 := test_runner.Rollback(); err2 != nil {
				test_runner.Logger().Println(au.BrightRed("error rolling back test database"), err2)
			}
		}
	}()

	for _, namespace := range namespaces.Values() {

		if err = test_runner.SavePoint("test_mutations"); err != nil {
			return err
		}

		for _, revision := range namespace.Revisions {
			if err = TestMutationsInTestDatabase(test_runner, revision); err != nil {
				return err
			}

			if err = test_runner.RollbackToSavepoint("test_mutations"); err != nil {
				return err
			}
		}

	}

	if err = test_runner.Rollback(); err != nil {
		return err
	}
	rollbacked = true

	return nil
}

func TestMutationsInTestDatabase(test_runner Executor, set *MutationSet) error {

	for _, role := range set.Roles.Values() {
		// We cheat, because we don't want to compare the roles, but we need them to exist anyway for the purpose of the tests
		if err := test_runner.Exec(fmt.Sprintf(`DO $$ BEGIN
		  IF NOT EXISTS ( SELECT FROM pg_catalog.pg_roles WHERE rolname = '%s' )
			THEN CREATE ROLE %s LOGIN;
			END IF;
			END $$ LANGUAGE plpgsql;`, role, pgx.Identifier{role}.Sanitize())); err != nil {
			return err
		}
	}

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

	// This test may not be necessary : any forgotten dependencies will be detected by now
	// if err := TestDowningMutations(test_runner, set); err != nil {
	// 	return err
	// }

	// Undo it all

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

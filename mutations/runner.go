package mutations

import (
	"log"

	au "github.com/logrusorgru/aurora"
	"github.com/ugurcsen/gods-generic/sets/hashset"
)

type Executor interface {

	// ReconcileRoles reconciles the roles in the database with the roles in the mutations.
	// Removing a role from the database will result in dropping all meta mutations before reconciling them.
	AddRole(namespace string, role string) error
	RemoveRole(namespace string, role string) error
	OverwriteRoles(namespace string, roles *hashset.Set[string]) error
	GetTestExecutor() (Executor, error)

	Logger() *log.Logger

	Exec(sql string, args ...interface{}) error
	Begin() error
	Rollback() error
	Commit() error
	SavePoint(name string) error
	RollbackToSavepoint(name string) error

	GetDBMutationsFromDb(namespace string) (*MutationSet, error)

	ClearMutations(namespace string) error
	Run(runnable *Runnable) error
	SaveMutation(mut *Mutation) error
	DeleteMutation(mut *Mutation) error
	Close() error
}

type MutationRunnerOptions struct {
	TestBefore bool
	Commit     bool
	Override   bool
}

func (o *MutationRunnerOptions) Merge(others ...*MutationRunnerOptions) {
	for _, other := range others {
		o.TestBefore = o.TestBefore || other.TestBefore
		o.Commit = o.Commit || other.Commit
		o.Override = o.Override || other.Override
	}
}

func RunMutations(runner Executor, local *MutationSet, opts ...*MutationRunnerOptions) error {

	var options = MutationRunnerOptions{}
	options.Merge(opts...)

	var err error
	// var test_runner Executor
	var namespace = local.Namespace
	var distant *MutationSet

	// if options.TestBefore {
	// 	// Create the test runner before BEGIN, so that we have the test database ready before modifying the roles.
	// 	test_runner, err = runner.GetTestExecutor()
	// 	if err != nil {
	// 		return err
	// 	}
	// 	defer test_runner.Close()
	// }

	if distant, err = runner.GetDBMutationsFromDb(namespace); err != nil {
		return err
	}

	sql_down, sql_up := local.GetMutationsDelta(distant, ITER_SQL)
	meta_down, meta_up := local.GetMutationsDelta(distant, ITER_META)

	var new_roles = local.Roles.Difference(distant.Roles)
	var removed_roles = distant.Roles.Difference(local.Roles)

	if sql_down.Size() > 0 || removed_roles.Size() > 0 {
		_, full_meta_up := local.GetMutationsDelta(nil, ITER_META)
		full_meta_down, _ := distant.GetMutationsDelta(nil, ITER_META)
		meta_down = full_meta_down
		meta_up = full_meta_up
	}

	// Unfortunately, we have to create missing roles outside of the BEGIN transaction, because the test runner will be in another database and thus cannot access the new roles as transactions cannot span databases.
	for _, role := range new_roles.Values() {
		if err := runner.AddRole(namespace, role); err != nil {
			return err
		}
	}

	if err := runner.Begin(); err != nil {
		return err
	}

	// 1. Start by downing the meta
	if err := meta_down.Run(runner); err != nil {
		return err
	}

	// Now, we're inside a transaction, so we can remove the roles without fear of having to undo stuff manually
	if removed_roles.Size() > 0 {
		for _, role := range removed_roles.Values() {
			if err := runner.RemoveRole(namespace, role); err != nil {
				return err
			}
		}
	}

	if err := sql_down.Run(runner); err != nil {
		return err
	}

	if err := sql_up.Run(runner); err != nil {
		return err
	}

	if err := meta_up.Run(runner); err != nil {
		return err
	}

	// if options.TestBefore {
	// 	if err := test_runner.Begin(); err != nil {
	// 		return err
	// 	}

	// 	if err := MutationTestSequence(test_runner, local, ITER_SQL); err != nil {
	// 		return err
	// 	}

	// 	if err := test_runner.Rollback(); err != nil {
	// 		return err
	// 	}

	// 	if err := test_runner.Close(); err != nil {
	// 		return err
	// 	}
	// }

	// if options.TestBefore {

	// 	if err := runner.SavePoint("test_downing_mutations"); err != nil {
	// 		return err
	// 	}

	// 	if err := TestDowningMutations(runner, local); err != nil {
	// 		return err
	// 	}

	// 	if err := runner.RollbackToSavepoint("test_downing_mutations"); err != nil {
	// 		return err
	// 	}
	// }

	runner.Logger().Println(au.BrightGreen("🎉"), "no errors")
	if options.Commit {
		runner.Logger().Println(au.BrightGreen("💾"), "committing")
		if err := runner.Commit(); err != nil {
			return err
		}
	}

	return nil
}

// // Apply the mutations on the disk to the database.
// func ApplyMutations(runner Executor, local *MutationSet) error {

// 	namespace := local.Namespace
// 	// Get the currently defined mutations so that we send them to the test runner
// 	current, err := runner.GetDBMutationsFromDb(namespace)
// 	if err != nil {
// 		return err
// 	}

// 	to_up_meta, to_down_meta := local.GetMutationsDelta(current, ITER_META)
// 	to_up_sql, to_down_sql := local.GetMutationsDelta(current, ITER_SQL)

// 	for _, down_runnable := range to_down_meta.Values() {
// 		if err := runner.Run(down_runnable); err != nil {
// 			return err
// 		}
// 	}

// 	for _, down_runnable := range to_down_sql.Values() {
// 		if err := runner.Run(down_runnable); err != nil {
// 			return err
// 		}
// 	}

// 	for _, up_runnable := range to_up_sql.Values() {
// 		if err := runner.Run(up_runnable); err != nil {
// 			return err
// 		}
// 	}

// 	for _, up_runnable := range to_up_meta.Values() {
// 		if err := runner.Run(up_runnable); err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

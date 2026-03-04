package mutations

import (
	"embed"
	"log"

	au "github.com/logrusorgru/aurora"
	"github.com/ugurcsen/gods-generic/sets/hashset"
)

// go:embed dmut-mutations/*
var dmut_mutations embed.FS

type Runner interface {

	// ReconcileRoles reconciles the roles in the database with the roles in the mutations.
	// Removing a role from the database will result in dropping all meta mutations before reconciling them.
	AddRole(namespace string, role string) error
	RemoveRole(namespace string, role string) error
	OverwriteRoles(namespace string, roles *hashset.Set[string]) error
	GetTestRunner() (Runner, error)

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

func RunMutations(runner Runner, local *MutationSet, opts ...*MutationRunnerOptions) error {

	var options = MutationRunnerOptions{}
	options.Merge(opts...)

	var err error
	var test_runner Runner
	var namespace = local.Namespace

	if options.TestBefore {
		// Create the test runner before BEGIN, so that we have the test database ready before modifying the roles.
		test_runner, err = runner.GetTestRunner()
		if err != nil {
			return err
		}
		defer test_runner.Close()
	}

	db, err := runner.GetDBMutationsFromDb(namespace)
	if err != nil {
		return err
	}

	var new_roles = local.Roles.Difference(db.Roles)
	var removed_roles = db.Roles.Difference(local.Roles)

	if !options.Override {
		// Unfortunately, we have to create missing roles outside of the BEGIN transaction, because the test runner will be in another database and thus cannot access the new roles as transactions cannot span databases.
		for _, role := range new_roles.Values() {
			if err := runner.AddRole(namespace, role); err != nil {
				return err
			}
		}

		defer func() {
			if err != nil {
				if err := runner.Rollback(); err != nil {
					runner.Logger().Println(au.BrightRed("💥"), "error rolling back in defer", err)
				}
				for _, role := range new_roles.Values() {
					if err := runner.RemoveRole(namespace, role); err != nil {
						runner.Logger().Println(au.BrightRed("💥"), "error removing role in defer", role, err)
					}
				}
			}
		}()
	} else {
		if err := runner.OverwriteRoles(namespace, local.Roles); err != nil {
			return err
		}
	}

	if err := runner.Begin(); err != nil {
		return err
	}

	down_meta, _ := local.GetMutationsDelta(db, ITER_META)
	for _, down_runnable := range down_meta.Values() {
		if err := runner.Run(down_runnable); err != nil {
			return err
		}
	}

	// Now, we're inside a transaction, so we can remove the roles without fear of having to undo stuff manually
	if removed_roles.Size() > 0 {
		for _, role := range removed_roles.Values() {
			if err := runner.RemoveRole(namespace, role); err != nil {
				return err
			}
		}
	}

	if options.TestBefore {
		if err := test_runner.Begin(); err != nil {
			return err
		}

		if err := MutationTestSequence(test_runner, local, ITER_SQL); err != nil {
			return err
		}

		if err := test_runner.Rollback(); err != nil {
			return err
		}

		if err := test_runner.Close(); err != nil {
			return err
		}
	}

	if options.Override {
		if err := runner.ClearMutations(namespace); err != nil {
			return err
		}

		for mut := range local.AllMutations() {
			if err := runner.SaveMutation(mut); err != nil {
				return err
			}
			runner.Logger().Println(au.BrightGreen("✓"), mut.DisplayName())
		}
	} else {
		if err := ApplyMutations(runner, local); err != nil {
			return err
		}
	}

	if options.TestBefore {

		if err := runner.SavePoint("test_downing_mutations"); err != nil {
			return err
		}

		if err := TestDowningMutations(runner, local); err != nil {
			return err
		}

		if err := runner.RollbackToSavepoint("test_downing_mutations"); err != nil {
			return err
		}
	}

	runner.Logger().Println(au.BrightGreen("🎉"), "no errors")
	if options.Commit {
		runner.Logger().Println(au.BrightGreen("💾"), "committing")
		if err := runner.Commit(); err != nil {
			return err
		}
	}

	return nil
}

// Apply the mutations on the disk to the database.
func ApplyMutations(runner Runner, local *MutationSet) error {

	namespace := local.Namespace
	// Get the currently defined mutations so that we send them to the test runner
	current, err := runner.GetDBMutationsFromDb(namespace)
	if err != nil {
		return err
	}

	to_up_meta, to_down_meta := local.GetMutationsDelta(current, ITER_META)
	to_up_sql, to_down_sql := local.GetMutationsDelta(current, ITER_SQL)

	for _, down_runnable := range to_down_meta.Values() {
		if err := runner.Run(down_runnable); err != nil {
			return err
		}
	}

	for _, down_runnable := range to_down_sql.Values() {
		if err := runner.Run(down_runnable); err != nil {
			return err
		}
	}

	for _, up_runnable := range to_up_sql.Values() {
		if err := runner.Run(up_runnable); err != nil {
			return err
		}
	}

	for _, up_runnable := range to_up_meta.Values() {
		if err := runner.Run(up_runnable); err != nil {
			return err
		}
	}

	return nil
}

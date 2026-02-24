package mutations

import (
	"log"

	mapset "github.com/deckarep/golang-set/v2"
	au "github.com/logrusorgru/aurora"
)

type Runner interface {
	// ReconcileRoles reconciles the roles in the database with the roles in the mutations.
	// Removing a role from the database will result in dropping all meta mutations before reconciling them.
	ReconcileRoles(roles mapset.Set[string], override bool) error
	GetTestRunner() (Runner, error)

	Logger() *log.Logger

	Begin() error
	Rollback() error
	Commit() error
	SavePoint(name string) error
	RollbackToSavepoint(name string) error

	GetDBMutationsFromDb() (DbMutationMap, error)

	ClearMutations() error
	SaveMutation(mut *DbMutation) error
	ApplyMutation(mut *DbMutation) error
	UndoMutation(mut *DbMutation) error
	Close() error

	// InstallDmut() error
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

func RunMutations(runner Runner, disk_mutations DbMutationMap, disk_roles mapset.Set[string], opts ...*MutationRunnerOptions) error {

	var options = MutationRunnerOptions{}
	options.Merge(opts...)

	var err error
	var test_runner Runner

	if options.TestBefore {
		// Create the test runner before BEGIN, so that we have the test database ready before modifying the roles.
		test_runner, err = runner.GetTestRunner()
		if err != nil {
			return err
		}
	}

	if err := runner.Begin(); err != nil {
		return err
	}

	if err := runner.ReconcileRoles(disk_roles, options.Override); err != nil {
		return err
	}

	if options.TestBefore {
		if err := test_runner.Begin(); err != nil {
			return err
		}

		if err := TestLeafMutations(test_runner, disk_mutations); err != nil {
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
		if err := runner.ClearMutations(); err != nil {
			return err
		}

		for _, mut := range disk_mutations.GetMutationsInOrder(true) {
			if err := runner.SaveMutation(mut); err != nil {
				return err
			}
			runner.Logger().Println(au.BrightGreen("✓"), mut.DisplayName())
		}
	} else {
		if err := ApplyMutations(runner, disk_mutations); err != nil {
			return err
		}
	}

	if options.TestBefore {

		if err := runner.SavePoint("test_downing_mutations"); err != nil {
			return err
		}

		if err := TestDowningMutations(runner); err != nil {
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

	if err := runner.Close(); err != nil {
		return err
	}

	return nil
}

// Apply the mutations on the disk to the database.
func ApplyMutations(runner Runner, disk_mutations DbMutationMap) error {

	// Get the currently defined mutations so that we send them to the test runner
	current, err := runner.GetDBMutationsFromDb()
	if err != nil {
		return err
	}

	var defunct_db_hashes = current.GetUniqueHashes(disk_mutations)

	// Undo the defunct mutations in the database
	for _, mut := range current.GetMutationsInOrder(false, defunct_db_hashes.ToSlice()...) {
		if err := runner.UndoMutation(mut); err != nil {
			return err
		}

		runner.Logger().Println(au.BrightRed("🗑"), mut.DisplayName())
		current.Remove(mut)
	}

	// Check what new hashes are in the local mutations after the removed database ones are gone.
	var new_local_hashes = disk_mutations.GetUniqueHashes(current)

	for _, mut := range disk_mutations.GetMutationsInOrder(true, new_local_hashes.ToSlice()...) {
		if err := runner.ApplyMutation(mut); err != nil {
			return err
		}
		runner.Logger().Println(au.BrightGreen("✓"), mut.DisplayName())
	}

	return nil
}

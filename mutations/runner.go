package mutations

import (
	mapset "github.com/deckarep/golang-set/v2"
)

type Runner interface {
	IsTesting() bool
	// ReconcileRoles reconciles the roles in the database with the roles in the mutations.
	// Removing a role from the database will result in dropping all meta mutations before reconciling them.
	ReconcileRoles(roles mapset.Set[string]) error
	GetTestRunner() (Runner, error)

	Begin() error
	Rollback() error
	Commit() error
	SavePoint(name string) error
	RollbackToSavepoint(name string) error

	GetDBMutationsFromDb() (DbMutationMap, error)

	ApplyMutation(mut *DbMutation) error
	UndoMutation(mut *DbMutation) error
	Close() error

	// InstallDmut() error
}

func ApplyMutations(runner Runner, disk_mutations DbMutationMap, disk_roles mapset.Set[string]) error {

	// Create the test runner before BEGIN, so that we have the test database ready before modifying the roles.
	test_runner, err := runner.GetTestRunner()
	if err != nil {
		return err
	}

	if err := runner.Begin(); err != nil {
		return err
	}

	if err := runner.ReconcileRoles(disk_roles); err != nil {
		return err
	}

	if err := TestMutationsFromOriginal(test_runner, runner, disk_mutations); err != nil {
		return err
	}

	if err := ApplyMutationsFromCurrent(runner, disk_mutations); err != nil {
		return err
	}

	// Commit ?
	return nil
}

// Apply the mutations on the disk to the database.
func ApplyMutationsFromCurrent(runner Runner, disk_mutations DbMutationMap) error {

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

		current.Remove(mut)
	}

	// Check what new hashes are in the local mutations after the removed database ones are gone.
	var new_local_hashes = disk_mutations.GetUniqueHashes(current)

	for _, mut := range disk_mutations.GetMutationsInOrder(true, new_local_hashes.ToSlice()...) {
		if err := runner.ApplyMutation(mut); err != nil {
			return err
		}
	}

	return nil
}

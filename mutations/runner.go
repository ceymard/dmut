package mutations

import (
	"log"

	mapset "github.com/deckarep/golang-set/v2"
	au "github.com/logrusorgru/aurora"
)

type Runner interface {
	IsTesting() bool
	// ReconcileRoles reconciles the roles in the database with the roles in the mutations.
	// Removing a role from the database will result in dropping all meta mutations before reconciling them.
	ReconcileRoles(roles mapset.Set[string]) error

	Commit() error
	SavePoint(name string) error
	RollbackToSavepoint(name string) error

	GetDBMutationsFromDb() (DbMutationMap, error)

	ApplyMutation(mut *DbMutation) error
	UndoMutation(mut *DbMutation) error
}

func ApplyMutations(runner Runner, disk_mutations DbMutationMap, disk_roles mapset.Set[string]) error {
	if err := runner.ReconcileRoles(disk_roles); err != nil {
		return err
	}

	current, err := runner.GetDBMutationsFromDb()
	if err != nil {
		return err
	}

	return ApplyMutationsFromCurrent(runner, disk_mutations, current)
}

// Apply the mutations on the disk to the database.
func ApplyMutationsFromCurrent(runner Runner, disk_mutations DbMutationMap, current DbMutationMap) error {
	// Make a copy of the current mutations to avoid modifying the original because we're going to remove mutations from it
	current = current.Copy()

	var defunct_db_hashes = current.GetUniqueHashes(disk_mutations)

	// Undo the defunct mutations in the database
	for _, mut := range current.GetMutationsInOrder(false, defunct_db_hashes.ToSlice()...) {
		if err := runner.UndoMutation(mut); err != nil {
			return err
		}

		current.Remove(mut)

		if !runner.IsTesting() {
			log.Println(au.Green("▼"), mut.Name)
		}
	}

	// Check what new hashes are in the local mutations after the removed database ones are gone.
	var new_local_hashes = disk_mutations.GetUniqueHashes(current)

	for _, mut := range disk_mutations.GetMutationsInOrder(true, new_local_hashes.ToSlice()...) {
		if err := runner.ApplyMutation(mut); err != nil {
			return err
		}

		if !runner.IsTesting() {
			log.Println(au.Green("▲"), mut.Name)
		}
	}

	return nil
}

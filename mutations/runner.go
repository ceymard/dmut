package mutations

import (
	"log"

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
	ReleaseSavepoint(name string) error

	GetDBMutationsFromDb(namespace string) (*MutationSet, error)

	ClearMutations(namespace string) error
	Run(runnable *Runnable) error
	SaveMutation(mut *Mutation) error
	DeleteMutation(mut *Mutation) error
	Close() error
}

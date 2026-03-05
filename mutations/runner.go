package mutations

import (
	"log"
)

type Executor interface {
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
	GetTestOutput() string
}

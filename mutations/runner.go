package mutations

import (
	"log"
)

type Executor interface {
	Logger() *log.Logger
	GetTestExecutor() Executor
	GetTestOutput() string

	GetStringOutput() string

	Exec(sql string, args ...interface{}) error

	Begin() error
	Rollback() error
	Commit() error
	SavePoint(name string) error
	RollbackToSavepoint(name string) error
	ReleaseSavepoint(name string) error

	GetDBMutationsFromDb(namespace string) (*MutationSet, error)

	ClearMutations(namespace string) error
	SaveMutations(mutations *MutationSet) error

	Run(runnable *Runnable) error
	Close() error
}

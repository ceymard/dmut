package mutations

import (
	"log/slog"
)

type Executor interface {
	// Logger is where the runner and the functions driving it log. Records
	// logged between BeginCapture and EndCapture are held back.
	Logger() *slog.Logger
	// BeginCapture holds back all records logged through Logger() until EndCapture.
	BeginCapture()
	// EndCapture ends the capture; held records are emitted if replay is true,
	// dropped otherwise.
	EndCapture(replay bool)
	Exec(sql string, args ...interface{}) error

	Begin() error
	Rollback() error
	Commit() error
	SavePoint(name string) error
	RollbackToSavepoint(name string) error
	ReleaseSavepoint(name string) error

	GetDBMutationsFromDb(namespace string) (*MutationSet, error)
	GetDBNamespaces() ([]string, error)

	ClearMutations(namespace string) error
	DeleteMutations(namespace string, names []string) error
	SaveMutations(mutations *MutationSet) error

	Run(runnable *Runnable) error
	Close() error
}

package mutations

import (
	"context"
	"log/slog"
	"time"
)

// CleanupTimeout bounds the work done after a cancellation (rolling back,
// closing the connection). When it expires the connection is dropped and
// Postgres rolls back the open transaction by itself.
const CleanupTimeout = 5 * time.Second

// CleanupContext returns a context for cleanup work: it keeps the values of
// ctx but ignores its cancellation, and expires after CleanupTimeout. The
// caller must call the returned cancel function.
func CleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), CleanupTimeout)
}

// Executor drives a database. Every method that talks to the database takes a
// context.Context: cancelling it aborts the statement in flight and makes the
// method return an error wrapping ctx.Err().
type Executor interface {
	// Logger is where the runner and the functions driving it log. Records
	// logged between BeginCapture and EndCapture are held back.
	Logger() *slog.Logger
	// BeginCapture holds back all records logged through Logger() until EndCapture.
	BeginCapture()
	// EndCapture ends the capture; held records are emitted if replay is true,
	// dropped otherwise.
	EndCapture(replay bool)
	Exec(ctx context.Context, sql string, args ...interface{}) error

	Begin(ctx context.Context) error
	Rollback(ctx context.Context) error
	Commit(ctx context.Context) error
	SavePoint(ctx context.Context, name string) error
	RollbackToSavepoint(ctx context.Context, name string) error
	ReleaseSavepoint(ctx context.Context, name string) error

	GetDBMutationsFromDb(ctx context.Context, namespace string) (*MutationSet, error)
	GetDBNamespaces(ctx context.Context) ([]string, error)

	ClearMutations(ctx context.Context, namespace string) error
	DeleteMutations(ctx context.Context, namespace string, names []string) error
	SaveMutations(ctx context.Context, mutations *MutationSet) error

	Run(ctx context.Context, runnable *Runnable) error
	// Close ends the connection. It ignores any cancellation of ctx and is
	// bounded by CleanupTimeout, so it is safe to defer after a cancellation.
	Close(ctx context.Context) error
}

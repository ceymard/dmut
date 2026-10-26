package mutations

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/samber/oops"
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

// Runner is one connection to the database, with the logger that goes with it.
// It is not safe for concurrent use. Every method that talks to the database takes
// a context: cancelling it aborts the statement in flight.
type Runner struct {
	conn   *pgx.Conn
	logger *slog.Logger
	state  *captureState
}

// location says where a statement comes from, to put in the error if it fails.
type location struct {
	Namespace string
	Mutation  string
	File      string
	Position  int
}

// NewRunner connects to the database at url. Progress, notices and SQL (at debug level)
// are logged through logger, or through slog.Default() when logger is nil. ctx only
// bounds the initial connection. The password of url is never logged.
func NewRunner(ctx context.Context, url string, logger *slog.Logger) (*Runner, error) {
	if logger == nil {
		logger = slog.Default()
	}
	state := &captureState{}
	r := &Runner{state: state, logger: slog.New(captureHandler{state: state, inner: logger.Handler()})}

	r.logger.Info("connecting", "uri", redactURI(url))
	config, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, oops.In("pg").Wrap(err)
	}
	config.OnNotice = func(_ *pgconn.PgConn, notice *pgconn.Notice) {
		r.logger.Info(notice.Message, "notice", true, "severity", notice.Severity, "sqlstate", notice.Code)
	}
	if r.conn, err = pgx.ConnectConfig(ctx, config); err != nil {
		return nil, oops.In("pg").Wrap(err)
	}
	return r, nil
}

// Logger returns the logger of the runner. While a capture is active (see
// BeginCapture) its records are held back instead of being emitted.
func (r *Runner) Logger() *slog.Logger { return r.logger }

// Close ends the connection. It ignores any cancellation of ctx and is bounded by
// CleanupTimeout, so it is safe to defer after a cancellation.
func (r *Runner) Close(ctx context.Context) error {
	ctx, cancel := CleanupContext(ctx)
	defer cancel()
	return r.conn.Close(ctx)
}

// BeginCapture holds back the records capture accepts, logged through Logger()
// from now on, until EndCapture. Captures don't nest.
func (r *Runner) BeginCapture(capture Capture) { r.state.beginCapture(capture) }

// EndCapture stops the capture started by BeginCapture. The held records are emitted in
// order if replay is true, and dropped otherwise. A non-empty note is logged before a
// replay that has records.
func (r *Runner) EndCapture(replay bool, note string) {
	var announce func()
	if note != "" {
		announce = func() { r.logger.Info(note) }
	}
	r.state.endCapture(replay, announce)
}

// wrapPgError attaches the failing sql and, for server errors, the position, detail,
// hint and SQLSTATE as context values (see ErrorAttrs).
func wrapPgError(err error, sql string) error {
	if err == nil {
		return nil
	}
	oo := oops.In("pg").Code("pg_error").With("sql", sql)
	if prep, ok := err.(*pgconn.PrepareError); ok {
		err = prep.Unwrap()
	}
	if pgerr, ok := err.(*pgconn.PgError); ok {
		oo = oo.With("sqlstate", pgerr.Code)
		if pgerr.Detail != "" {
			oo = oo.With("detail", pgerr.Detail)
		}
		if pgerr.Hint != "" {
			oo = oo.With("hint", pgerr.Hint)
		}
		if pgerr.Position > 0 {
			oo = oo.With("position", int(pgerr.Position))
		}
	}
	return oo.Wrap(err)
}

// Exec runs sql with the given arguments.
func (r *Runner) Exec(ctx context.Context, sql string, args ...any) error {
	return r.execAt(ctx, location{}, sql, args...)
}

func (r *Runner) execAt(ctx context.Context, loc location, sql string, args ...any) error {
	r.logger.Debug("sql", "sql", sql)
	if _, err := r.conn.Exec(ctx, sql, args...); err != nil {
		return locate(loc, wrapPgError(err, sql))
	}
	return nil
}

// Begin starts the transaction a run happens in.
func (r *Runner) Begin(ctx context.Context) error { return r.Exec(ctx, "BEGIN") }

// Commit commits the transaction.
func (r *Runner) Commit(ctx context.Context) error {
	if err := r.Exec(ctx, "COMMIT"); err != nil {
		return err
	}
	r.logger.Info("committed", IconKey, IconCommitted)
	return nil
}

// Rollback rolls the transaction back without logging: whether it is a failure worth an
// icon or a deliberate rollback is for the caller to say.
func (r *Runner) Rollback(ctx context.Context) error { return r.Exec(ctx, "ROLLBACK") }

// SavePoint opens the named savepoint.
func (r *Runner) SavePoint(ctx context.Context, name string) error {
	return r.Exec(ctx, "SAVEPOINT "+name)
}

// RollbackToSavepoint undoes everything since the named savepoint, which stays open.
func (r *Runner) RollbackToSavepoint(ctx context.Context, name string) error {
	return r.Exec(ctx, "ROLLBACK TO SAVEPOINT "+name)
}

// ReleaseSavepoint keeps the work done since the named savepoint.
func (r *Runner) ReleaseSavepoint(ctx context.Context, name string) error {
	return r.Exec(ctx, "RELEASE SAVEPOINT "+name)
}

package mutations

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/samber/oops"
)

var _ Executor = &PgRunner{}

type PgRunner struct {
	uri    string
	logger *slog.Logger
	core   *captureCore
	conn   *pgx.Conn
}

// Logger returns the runner's logger. While a capture is active (see
// BeginCapture) its records are held back instead of being emitted.
func (r *PgRunner) Logger() *slog.Logger {
	return r.logger
}

// Close ends the connection. It ignores any cancellation of ctx and is
// bounded by CleanupTimeout, so it is safe to defer after a cancellation.
func (r *PgRunner) Close(ctx context.Context) error {
	ctx, cancel := CleanupContext(ctx)
	defer cancel()
	return r.conn.Close(ctx)
}

// NewPgRunner connects to the database at url. Progress, notices and SQL
// (at debug level) are logged through logger, or through slog.Default() when
// logger is nil. ctx only bounds the initial connection. The password of url is never logged.
func NewPgRunner(ctx context.Context, url string, logger *slog.Logger) (*PgRunner, error) {
	if logger == nil {
		logger = slog.Default()
	}
	core := &captureCore{}
	res := &PgRunner{uri: url, core: core}
	res.logger = slog.New(captureHandler{core: core, inner: logger.Handler()})

	res.logger.Info("connecting", "uri", redactURI(url))
	config, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	config.OnNotice = pgconn.NoticeHandler(func(conn *pgconn.PgConn, notice *pgconn.Notice) {
		res.logger.Info(notice.Message, "notice", true, "severity", notice.Severity, "sqlstate", notice.Code)
	})

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	res.conn = conn
	return res, nil
}

// BeginCapture holds back every record logged through Logger() from now on,
// tagged phase=test, until EndCapture.
func (r *PgRunner) BeginCapture() {
	r.core.beginCapture()
}

// EndCapture stops the capture started by BeginCapture. The held records are
// emitted in order if replay is true, and dropped otherwise.
func (r *PgRunner) EndCapture(replay bool) {
	r.core.endCapture(replay)
}

// wrapPgError attaches the failing sql and, for server errors, the position,
// detail, hint and SQLSTATE as raw context values (see ErrorAttrs). Colouring
// the sql around position is the renderer's job, not the error's.
func wrapPgError(err error, sql string) error {
	if err == nil {
		return nil
	}
	oo := oops.In("pg").Code("pg_error").With("sql", sql)
	if pgerr, ok := err.(*pgconn.PrepareError); ok {
		err = pgerr.Unwrap()
	}

	if pgerr, ok := err.(*pgconn.PgError); ok {
		oo = oo.With("sqlstate", pgerr.Code)
		if details := pgerr.Detail; details != "" {
			oo = oo.With("detail", details)
		}
		if hint := pgerr.Hint; hint != "" {
			oo = oo.With("hint", hint)
		}
		if pgerr.Position > 0 {
			oo = oo.With("position", int(pgerr.Position))
		}
	}

	return oo.Wrap(err)
}

func (r *PgRunner) Run(ctx context.Context, runnable *Runnable) error {
	if runnable.Size() == 0 {
		return nil
	}
	r.logger.Info("run", runnable.LogAttrs()...)
	for i, stmt := range runnable.Statements() {
		if err := r.exec(ctx, runnable.Mutation, stmt); err != nil {
			return oops.With("statement_index", i+1).Wrap(err)
		}
	}
	return nil
}

func (r *PgRunner) ClearMutations(ctx context.Context, namespace string) error {
	sql := `DELETE FROM __dmut__.mutations WHERE namespace = $1`
	err := r.exec(ctx, nil, sql, namespace)
	return wrapPgError(err, sql)
}

// DeleteMutations removes bookkeeping for specific mutations, unlike ClearMutations
// which wipes an entire namespace - this is what a revision's `delete:` key uses to
// untrack named mutations without running a down for them.
func (r *PgRunner) DeleteMutations(ctx context.Context, namespace string, names []string) error {
	if len(names) == 0 {
		return nil
	}
	sql := `DELETE FROM __dmut__.mutations WHERE namespace = $1 AND name = ANY($2)`
	err := r.exec(ctx, nil, sql, namespace, names)
	return wrapPgError(err, sql)
}

func (r *PgRunner) Commit(ctx context.Context) error {
	if err := r.exec(ctx, nil, `COMMIT`); err != nil {
		return err
	}
	r.logger.Info("committed", IconKey, IconCommitted)
	return nil
}

func (r *PgRunner) Begin(ctx context.Context) error {
	return r.exec(ctx, nil, "BEGIN")
}

// Rollback rolls the transaction back without logging: whether it is a failure
// worth an icon or a deliberate rollback is for the caller to say.
func (r *PgRunner) Rollback(ctx context.Context) error {
	return r.exec(ctx, nil, "ROLLBACK")
}

func (r *PgRunner) SavePoint(ctx context.Context, name string) error {
	if name == "" {
		if err := r.exec(ctx, nil, "BEGIN"); err != nil {
			return wrapPgError(err, "BEGIN")
		}
		return nil
	}
	return r.exec(ctx, nil, `SAVEPOINT `+name)
}

func (r *PgRunner) RollbackToSavepoint(ctx context.Context, name string) error {
	var cmd = `ROLLBACK`
	if name != "" {
		cmd += ` TO SAVEPOINT ` + name
	}
	return r.exec(ctx, nil, cmd)
}

func (r *PgRunner) ReleaseSavepoint(ctx context.Context, name string) error {
	return r.exec(ctx, nil, `RELEASE SAVEPOINT `+name)
}

func (r *PgRunner) Exec(ctx context.Context, sql string, args ...interface{}) error {
	return r.exec(ctx, nil, sql, args...)
}

func (r *PgRunner) exec(ctx context.Context, mutation *Mutation, sql string, args ...interface{}) error {
	r.logger.Debug("sql", "sql", sql)
	_, err := r.conn.Exec(ctx, sql, args...)
	if err != nil {
		err = wrapPgError(err, sql)
		oo := oops.In("pg")

		if mutation != nil {
			oo = oo.With("mutation", mutation.Name).With("file", mutation.File).With("namespace", mutation.Namespace).With("needs", mutation.Needs).With("glue_needs", mutation.GlueNeeds)
		}
		err = oo.Wrap(err)
	}
	return err
}

// GetDBNamespaces lists every distinct namespace recorded in __dmut__.mutations,
// excluding __dmut__ itself - used by `collect -r` when sourcing a revision shell
// from a live database with no local files to already know the namespace from.
func (r *PgRunner) GetDBNamespaces(ctx context.Context) ([]string, error) {
	var (
		db     = r.conn
		exists bool
	)

	sql := `SELECT EXISTS (
		SELECT 1
		FROM pg_catalog.pg_namespace
		WHERE nspname = '__dmut__'
	)`
	if err := db.QueryRow(ctx, sql).Scan(&exists); err != nil {
		return nil, wrapPgError(err, sql)
	}
	if !exists {
		return nil, nil
	}

	sql = `SELECT DISTINCT namespace FROM __dmut__.mutations WHERE namespace != $1 ORDER BY namespace`
	rows, err := db.Query(ctx, sql, DmutNamespace)
	if err != nil {
		return nil, wrapPgError(err, sql)
	}
	defer rows.Close()

	var namespaces []string
	for rows.Next() {
		var ns string
		if err := rows.Scan(&ns); err != nil {
			return nil, wrapPgError(err, sql)
		}
		namespaces = append(namespaces, ns)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapPgError(err, sql)
	}

	return namespaces, nil
}

// get the mutations already in the database
func (r *PgRunner) GetDBMutationsFromDb(ctx context.Context, namespace string) (*MutationSet, error) {
	var (
		db     = r.conn
		exists bool
	)

	// test for the presence of the __dmut__ schema by
	sql := `SELECT EXISTS (
		SELECT 1
		FROM pg_catalog.pg_namespace
		WHERE nspname = '__dmut__'
	)`
	if err := db.QueryRow(ctx, sql).Scan(&exists); err != nil {
		return nil, wrapPgError(err, sql)
	}
	if !exists {
		return NewMutationSet(namespace, 0, ""), nil
	}

	res := NewMutationSet(namespace, 0, "")

	// First, extract a list of already active mutations and check if they have to be downed because they're
	// either	 inexistant or their hash changed.
	sql = `select
	   json_build_object(
			'revision', coalesce(max(revision), 0),
			'mutations', coalesce(json_agg(row_to_json(r))::json, '[]'::json)
		) as data
		from (select * from __dmut__.mutations WHERE namespace = $1) r
	`
	row := db.QueryRow(ctx, sql, namespace)

	var json_text []byte
	if err := row.Scan(&json_text); err != nil {
		return nil, wrapPgError(err, sql)
	}

	type scanned struct {
		Revision  int         `json:"revision"`
		Mutations []*Mutation `json:"mutations"`
	}

	var sc scanned
	if err := json.Unmarshal(json_text, &sc); err != nil {
		return nil, oops.In("pg").Wrapf(err, "error unmarshalling mutations %s", json_text)
	}

	res.Revision = sc.Revision
	res.Namespace = namespace

	for _, mut := range sc.Mutations {
		res.AddMutation(mut)
	}

	if err := res.ResolveDependencies(); err != nil {
		return nil, err
	}

	return res, nil
}

func (r *PgRunner) SaveMutations(ctx context.Context, mutations *MutationSet) (err error) {
	if err = r.ClearMutations(ctx, mutations.Namespace); err != nil {
		return err
	}

	var muts []*Mutation = make([]*Mutation, 0, mutations.Size())
	for m := range mutations.AllMutations() {

		if m.ShouldBeSaved() {
			muts = append(muts, m)
		}
	}

	var muts_json []byte
	if muts_json, err = json.Marshal(muts); err != nil {
		return err
	}

	sql := `
		INSERT INTO __dmut__.mutations(
			namespace,
			revision,
			file,
			name,
			needs,
			glue_needs,
			glue,
			core
		)
		SELECT
			$2,
			$3,
			coalesce(file, ''),
			name,
			coalesce(needs, '{}'::text[]),
			coalesce(glue_needs, '{}'::text[]),
			coalesce(glue, '{}'::__dmut__.mutation_statement[]),
			coalesce(core, '{}'::__dmut__.mutation_statement[])
		FROM json_populate_recordset(NULL::__dmut__.mutations, $1::json)
		ON CONFLICT (namespace, name) DO UPDATE SET file = excluded.file, needs = excluded.needs, glue_needs = excluded.glue_needs, glue = excluded.glue, core = excluded.core`

	if err := r.exec(ctx, nil, sql, muts_json, mutations.Namespace, mutations.Revision); err != nil {
		return wrapPgError(err, sql)
	}
	return nil
}

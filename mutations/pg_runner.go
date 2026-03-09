package mutations

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	au "github.com/logrusorgru/aurora"
	"github.com/samber/oops"
)

var _ Executor = &PgRunner{}

type PgRunner struct {
	uri     string
	logger  *log.Logger
	conn    *pgx.Conn
	verbose bool
	buf     bytes.Buffer
}

func (r *PgRunner) Logger() *log.Logger {
	return r.logger
}

func (r *PgRunner) Close() error {
	return r.conn.Close(context.Background())
}

func NewPgRunner(url string, verbose bool) (*PgRunner, error) {

	res := &PgRunner{uri: url, verbose: verbose}

	res.logger = log.New(os.Stdout, "", log.Lshortfile|log.LstdFlags)
	res.logger.SetPrefix(au.BrightGreen("pg ").String())

	res.logger.Println("connecting to", url)
	conn, err := pgx.Connect(context.Background(), url)
	if err != nil {
		return nil, err
	}
	res.conn = conn
	return res, nil
}

func (r *PgRunner) GetTestExecutor() Executor {

	res := *r
	res.logger = log.New(os.Stdout, "", r.logger.Flags())
	res.logger.SetPrefix(au.BrightMagenta("test ").String())
	res.logger.SetOutput(&res.buf)

	return &res
}

func (r *PgRunner) GetStringOutput() string {
	return r.buf.String()
}

func (r *PgRunner) GetTestOutput() string {
	return r.buf.String()
}

func wrapPgError(err error, sql string) error {
	if err == nil {
		return nil
	}
	oo := oops.In("pg").Code("pg_error").With("sql", sql)
	if pgerr, ok := err.(*pgconn.PrepareError); ok {
		err = pgerr.Unwrap()
	}

	if pgerr, ok := err.(*pgconn.PgError); ok {
		if details := pgerr.Detail; details != "" {
			oo = oo.With("detail", details)
		}
		if hint := pgerr.Hint; hint != "" {
			oo = oo.With("hint", hint)
		}
		if pgerr.Position > 0 {
			oo = oo.With("sql", au.BrightBlue(sql[0:pgerr.Position-1]).String()+au.BrightRed(sql[pgerr.Position-1:]).String())
		} else {
			oo = oo.With("sql", au.BrightBlue(sql).String())
		}

		return oo.Wrap(err)
	}

	return oo.Wrap(err)
}

func (r *PgRunner) Run(runnable *Runnable) error {
	if runnable.Size() == 0 {
		return nil
	}
	r.logger.Println(runnable.DisplayName())
	for i, stmt := range runnable.Statements() {
		if err := r.exec(runnable.Mutation, stmt); err != nil {
			oo := oops.With("statement index", i+1)
			if oop2, ok := err.(*oops.OopsError); ok {
				if _, ok := oop2.Context()["statement"]; !ok {
					oo = oo.With("statement", au.BrightBlue(stmt).String())
				}
			}
			return oo.With("statement", stmt).Wrap(err)
		}
	}
	return nil
}

func (r *PgRunner) ClearMutations(namespace string) error {
	sql := `DELETE FROM __dmut__.mutations WHERE namespace = $1`
	err := r.exec(nil, sql, namespace)
	return wrapPgError(err, sql)
}

func (r *PgRunner) Commit() error {
	r.logger.Println(au.BrightGreen("💾"), "committing")
	return r.exec(nil, `COMMIT`)
}

func (r *PgRunner) Begin() error {
	// r.logger.Println(au.BrightGreen("💾"), "BEGIN")
	return r.exec(nil, "BEGIN")
}

func (r *PgRunner) Rollback() error {
	r.logger.Println(au.BrightRed("↩"), "rolling back")
	return r.exec(nil, "ROLLBACK")
}

func (r *PgRunner) SavePoint(name string) error {
	if name == "" {
		if err := r.exec(nil, "BEGIN"); err != nil {
			return wrapPgError(err, "BEGIN")
		}
		return nil
	}
	// r.logger.Println(au.BrightGreen("💾"), "saving point", name)
	return r.exec(nil, `SAVEPOINT `+name)
}

func (r *PgRunner) RollbackToSavepoint(name string) error {
	var cmd = `ROLLBACK`
	if name != "" {
		cmd += ` TO SAVEPOINT ` + name
	}
	// r.logger.Println(au.BrightRed("💾"), "rolling back to point", name)
	return r.exec(nil, cmd)
}

func (r *PgRunner) ReleaseSavepoint(name string) error {
	return r.exec(nil, `RELEASE SAVEPOINT `+name)
}

func (r *PgRunner) Exec(sql string, args ...interface{}) error {
	return r.exec(nil, sql, args...)
}

func (r *PgRunner) exec(mutation *Mutation, sql string, args ...interface{}) error {
	if r.verbose {
		r.logger.Println(au.BrightBlue(sql))
	}
	_, err := r.conn.Exec(context.Background(), sql, args...)
	if err != nil {
		err = wrapPgError(err, sql)
		oo := oops.In("pg")

		if mutation != nil {
			oo = oo.With("mutation", mutation.Name).With("file", mutation.File).With("namespace", mutation.Namespace).With("needs", mutation.Needs).With("meta_needs", mutation.MetaNeeds)

		}
		err = oo.Wrap(err)
		// err = oops.In("pg").With("sql", sql).Wrapf(err, "%s %s", au.BrightRed("error in statement").String(), au.BrightBlue(sql).String())
	}
	return err
}

// get the mutations already in the database
func (r *PgRunner) GetDBMutationsFromDb(namespace string) (*MutationSet, error) {
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
	if err := db.QueryRow(context.Background(), sql).Scan(&exists); err != nil {
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
	row := db.QueryRow(context.Background(), sql, namespace)

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

func (r *PgRunner) SaveMutations(mutations *MutationSet) (err error) {
	if err = r.ClearMutations(mutations.Namespace); err != nil {
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
			meta_needs,
			meta,
			sql,
			overriden
		)
		SELECT
			$2,
			$3,
			coalesce(file, ''),
			name,
			coalesce(needs, '{}'::text[]),
			coalesce(meta_needs, '{}'::text[]),
			coalesce(meta, '{}'::__dmut__.mutation_statement[]),
			coalesce(sql, '{}'::__dmut__.mutation_statement[]),
			coalesce(overriden, false)
		FROM json_populate_recordset(NULL::__dmut__.mutations, $1::json)
		ON CONFLICT (namespace, name) DO UPDATE SET file = excluded.file, needs = excluded.needs, meta_needs = excluded.meta_needs, meta = excluded.meta, sql = excluded.sql, overriden = excluded.overriden`

	if err := r.exec(nil, sql, muts_json, mutations.Namespace, mutations.Revision); err != nil {
		return wrapPgError(err, sql)
	}
	return nil
}

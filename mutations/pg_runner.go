package mutations

import (
	"bytes"
	"context"
	"log"
	"net/url"
	"os"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	au "github.com/logrusorgru/aurora"
	"github.com/samber/oops"
	"github.com/ugurcsen/gods-generic/sets/hashset"
)

var _ Executor = &PgRunner{}

type PgRunner struct {
	isTesting bool
	uri       string
	logger    *log.Logger
	conn      *pgx.Conn
	verbose   bool
	buf       bytes.Buffer
}

func (r *PgRunner) Logger() *log.Logger {
	return r.logger
}

func (r *PgRunner) Close() error {
	return r.conn.Close(context.Background())
}

func (r *PgRunner) IsTesting() bool {
	return r.isTesting
}

func NewPgRunner(url string, verbose bool) (*PgRunner, error) {

	res := &PgRunner{isTesting: false, uri: url, verbose: verbose}

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

func (r *PgRunner) GetTestExecutor() (Executor, error) {

	r.logger.Println(au.BrightGreen("🖥"), "creating test database")

	if err := r.exec(nil, `DROP DATABASE IF EXISTS __dmut_test__`); err != nil {
		return nil, err
	}

	if err := r.exec(nil, `CREATE DATABASE __dmut_test__`); err != nil {
		return nil, err
	}

	// replace the portion of the url after the URI with __dmut_test__ with URI manipulation library
	uri, err := url.Parse(r.uri)
	if err != nil {
		return nil, err
	}
	uri.Path = "__dmut_test__"
	new_url := uri.String()

	conn, err := pgx.Connect(context.Background(), new_url)
	if err != nil {
		return nil, err
	}

	res := &PgRunner{conn: conn, isTesting: true, uri: new_url, logger: log.New(os.Stdout, "", log.Lshortfile|log.LstdFlags), verbose: r.verbose}
	res.logger.SetPrefix(au.BrightMagenta("test ").String())

	res.logger.SetOutput(&res.buf)

	return res, nil
}

func (r *PgRunner) GetTestOutput() string {
	return r.buf.String()
}

func wrapPgError(err error) error {
	if err == nil {
		return nil
	}
	if pgerr, ok := err.(*pgconn.PgError); ok {
		oo := oops.In("pg").Code("pg_error")
		if details := pgerr.Detail; details != "" {
			oo = oo.With("detail", details)
		}
		if hint := pgerr.Hint; hint != "" {
			oo = oo.With("hint", hint)
		}
		return oo.Wrap(err)
	}
	return err
}

func (r *PgRunner) Run(runnable *Runnable) error {
	meta_or_not := au.BrightGreen("sql").String()
	if runnable.Direction.Meta {
		meta_or_not = au.BrightCyan("meta").String()
	}
	up_or_down := au.BrightGreen("up").String()
	if runnable.Direction.Down {
		up_or_down = au.BrightRed("down").String()
	}
	r.logger.Println(up_or_down, au.BrightMagenta(runnable.Mutation.set.Namespace).String(), runnable.Mutation.Name, meta_or_not)
	for i, stmt := range runnable.Statements() {
		if err := r.exec(runnable.Mutation, stmt); err != nil {
			return oops.With("statement index", i+1).With("statement", stmt).Wrap(err)
		}
	}
	return nil
}

func (r *PgRunner) DeleteMutation(m *Mutation) error {
	err := r.exec(m, `DELETE FROM __dmut__.mutations WHERE namespace = $1 AND name = $2`, m.set.Namespace, m.Name)
	return wrapPgError(err)
}

func (r *PgRunner) ClearMutations(namespace string) error {
	err := r.exec(nil, `DELETE FROM __dmut__.mutations WHERE namespace = $1`, namespace)
	return wrapPgError(err)
}

func (r *PgRunner) SaveMutation(m *Mutation) error {
	err := r.exec(m, `INSERT INTO __dmut__.mutations(namespace, file, name, needs, meta, sql, meta) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (namespace, name) DO UPDATE SET file = $2, needs = $4, meta = $5, sql = $6, meta = $7`, m.set.Namespace, m.File, m.Name, m.Needs, m.Meta, m.Sql, m.Meta)
	return wrapPgError(err)
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
	// r.logger.Println(au.BrightRed("💾"), "rolling back")
	return r.exec(nil, "ROLLBACK")
}

func (r *PgRunner) SavePoint(name string) error {
	if name == "" {
		if err := r.exec(nil, "BEGIN"); err != nil {
			return wrapPgError(err)
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
		oo := oops.In("pg")
		if pg_err, ok := err.(*pgconn.PgError); ok {
			if pg_err.Position > 0 {
				oo = oo.With("sql", au.BrightBlue(sql[0:pg_err.Position-1]).String()+au.BrightRed(sql[pg_err.Position-1:]).String())
			}
		} else {
			oo = oo.With("sql", au.BrightBlue(sql).String())
		}

		if mutation != nil {
			oo = oo.With("mutation", mutation.Name).With("file", mutation.File).With("namespace", mutation.Namespace).With("needs", mutation.Needs).With("meta_needs", mutation.MetaNeeds)

		}
		err = oo.Wrap(err)
		// err = oops.In("pg").With("sql", sql).Wrapf(err, "%s %s", au.BrightRed("error in statement").String(), au.BrightBlue(sql).String())
	}
	return err
}

func (r *PgRunner) getDbRoles(namespace string) (*hashset.Set[string], error) {
	var res = hashset.New[string]()
	rows, err := r.conn.Query(context.Background(), `SELECT name FROM __dmut__.roles WHERE namespace = $1`, namespace)
	if err != nil {
		return nil, wrapPgError(err)
	}
	defer rows.Close()

	for rows.Next() {
		var role string
		if err = rows.Scan(&role); err != nil {
			return nil, wrapPgError(err)
		}
		res.Add(role)
	}

	return res, nil
}

func (r *PgRunner) AddRole(namespace string, role string) error {
	if err := r.exec(nil, `CREATE ROLE `+pgx.Identifier{role}.Sanitize()); err != nil {
		return wrapPgError(err)
	}
	return r.exec(nil, `INSERT INTO __dmut__.roles(namespace, name) VALUES ($1, $2)`, namespace, role)
}

func (r *PgRunner) RemoveRole(namespace string, name string) error {
	if err := r.exec(nil, `DROP ROLE `+pgx.Identifier{name}.Sanitize()); err != nil {
		return wrapPgError(err)
	}
	return r.exec(nil, `DELETE FROM __dmut__.roles WHERE namespace = $1 AND name = $2`, namespace, name)
}

func (r *PgRunner) GetDBRoles(namespace string) (*hashset.Set[string], error) {
	return r.getDbRoles(namespace)
}

func (r *PgRunner) OverwriteRoles(namespace string, roles *hashset.Set[string]) error {
	if err := r.exec(nil, `DELETE FROM __dmut__.roles WHERE namespace = $1`, namespace); err != nil {
		return wrapPgError(err)
	}
	for _, role := range roles.Values() {
		if err := r.exec(nil, `INSERT INTO __dmut__.roles(namespace, rolname) VALUES ($1, $2)`, namespace, role); err != nil {
			return wrapPgError(err)
		}
	}
	return nil
}

// get the mutations already in the database
func (r *PgRunner) GetDBMutationsFromDb(namespace string) (*MutationSet, error) {
	var (
		db     = r.conn
		rows   pgx.Rows
		exists bool
		err    error
	)

	// test for the presence of the __dmut__ schema by
	if err := db.QueryRow(context.Background(), `SELECT EXISTS (
		SELECT 1
		FROM pg_catalog.pg_namespace
		WHERE nspname = '__dmut__'
	)`).Scan(&exists); err != nil {
		return nil, wrapPgError(err)
	}
	if !exists {
		return NewMutationSet(namespace, 0, ""), nil
	}

	res := NewMutationSet(namespace, 0, "")

	res.Roles, err = r.getDbRoles(namespace)
	if err != nil {
		return nil, err
	}

	// First, extract a list of already active mutations and check if they have to be downed because they're
	// either inexistant or their hash changed.
	if rows, err = db.Query(context.Background(), `select name, file, needs, meta_needs, meta, sql, meta from __dmut__.mutations WHERE namespace = $1`, namespace); err != nil {
		return nil, wrapPgError(err)
	}
	defer rows.Close()

	for rows.Next() {
		var dbmut Mutation
		if err = rows.Scan(&dbmut.Name, &dbmut.File, &dbmut.Needs, &dbmut.MetaNeeds, &dbmut.Meta, &dbmut.Sql, &dbmut.Meta); err != nil {
			return nil, wrapPgError(err)
		}
		res.AddMutation(&dbmut)
	}

	if err := res.ResolveDependencies(); err != nil {
		return nil, err
	}

	return res, nil
}

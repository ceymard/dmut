package mutations

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"os"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	au "github.com/logrusorgru/aurora"
	"github.com/pkg/errors"
	"github.com/ugurcsen/gods-generic/sets/hashset"
)

var _ Runner = &PgRunner{}

type PgRunner struct {
	isTesting bool
	uri       string
	logger    *log.Logger
	conn      *pgx.Conn
	verbose   bool
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

func (r *PgRunner) GetTestRunner() (Runner, error) {

	r.logger.Println(au.BrightGreen("🖥"), "creating test database")

	if err := r.exec(`DROP DATABASE IF EXISTS __dmut_test__`); err != nil {
		return nil, err
	}

	if err := r.exec(`CREATE DATABASE __dmut_test__`); err != nil {
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

	return res, nil
}

func wrapPgError(err error) error {
	if pgerr, ok := err.(*pgconn.PgError); ok {
		var details = pgerr.Detail
		return fmt.Errorf("%s (original error: %w)", details, err)
	}
	return err
}

func (r *PgRunner) Run(runnable *Runnable) error {
	for stmt := range runnable.Statements() {
		if err := r.exec(stmt); err != nil {
			return wrapPgError(err)
		}
	}
	return nil
}

func (r *PgRunner) DeleteMutation(m *Mutation) error {
	err := r.exec(`DELETE FROM dmut.mutations WHERE namespace = $1 AND name = $2`, m.set.Namespace, m.Name)
	return wrapPgError(err)
}

func (r *PgRunner) ClearMutations(namespace string) error {
	err := r.exec(`DELETE FROM dmut.mutations WHERE namespace = $1`, namespace)
	return wrapPgError(err)
}

func (r *PgRunner) SaveMutation(m *Mutation) error {
	err := r.exec(`INSERT INTO dmut.mutations(namespace, file, name, needs, meta, sql, meta) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (namespace, name) DO UPDATE SET file = $2, needs = $4, meta = $5, sql = $6, meta = $7`, m.set.Namespace, m.File, m.Name, m.Needs, m.Meta, m.Sql, m.Meta)
	return wrapPgError(err)
}

func (r *PgRunner) Commit() error {
	r.logger.Println(au.BrightGreen("💾"), "committing")
	return r.exec(`COMMIT`)
}

func (r *PgRunner) Begin() error {
	// r.logger.Println(au.BrightGreen("💾"), "BEGIN")
	return r.exec("BEGIN")
}

func (r *PgRunner) Rollback() error {
	// r.logger.Println(au.BrightRed("💾"), "rolling back")
	return r.exec("ROLLBACK")
}

func (r *PgRunner) SavePoint(name string) error {
	if name == "" {
		if err := r.exec("BEGIN"); err != nil {
			return wrapPgError(err)
		}
		return nil
	}
	// r.logger.Println(au.BrightGreen("💾"), "saving point", name)
	return r.exec(`SAVEPOINT ` + name)
}

func (r *PgRunner) RollbackToSavepoint(name string) error {
	var cmd = `ROLLBACK`
	if name != "" {
		cmd += ` TO SAVEPOINT ` + name
	}
	// r.logger.Println(au.BrightRed("💾"), "rolling back to point", name)
	return r.exec(cmd)
}

func (r *PgRunner) Exec(sql string, args ...interface{}) error {
	return r.exec(sql, args...)
}

func (r *PgRunner) exec(sql string, args ...interface{}) error {
	if r.verbose {
		r.logger.Println(au.BrightBlue(sql))
	}
	_, err := r.conn.Exec(context.Background(), sql, args...)
	if err != nil {
		err = errors.Wrap(err, au.BrightRed("error in statement").String()+" "+au.BrightBlue(sql).String())
	}
	return wrapPgError(err)
}

func (r *PgRunner) getDbRoles(namespace string) (*hashset.Set[string], error) {
	var res = hashset.New[string]()
	rows, err := r.conn.Query(context.Background(), `SELECT rolname FROM dmut.roles WHERE namespace = $1`, namespace)
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
	if err := r.exec(`CREATE ROLE ` + pgx.Identifier{role}.Sanitize()); err != nil {
		return wrapPgError(err)
	}
	return r.exec(`INSERT INTO dmut.roles(namespace, rolname) VALUES ($1, $2)`, namespace, role)
}

func (r *PgRunner) RemoveRole(namespace string, role string) error {
	if err := r.exec(`DROP ROLE ` + pgx.Identifier{role}.Sanitize()); err != nil {
		return wrapPgError(err)
	}
	return r.exec(`DELETE FROM dmut.roles WHERE namespace = $1 AND rolname = $2`, namespace, role)
}

func (r *PgRunner) GetDBRoles(namespace string) (*hashset.Set[string], error) {
	return r.getDbRoles(namespace)
}

func (r *PgRunner) OverwriteRoles(namespace string, roles *hashset.Set[string]) error {
	if err := r.exec(`DELETE FROM dmut.roles WHERE namespace = $1`, namespace); err != nil {
		return wrapPgError(err)
	}
	for _, role := range roles.Values() {
		if err := r.exec(`INSERT INTO dmut.roles(namespace, rolname) VALUES ($1, $2)`, namespace, role); err != nil {
			return wrapPgError(err)
		}
	}
	return nil
}

// get the mutations already in the database
func (r *PgRunner) GetDBMutationsFromDb(namespace string) (*MutationSet, error) {
	var (
		db   = r.conn
		rows pgx.Rows
		err  error
	)

	res := NewMutationSet(namespace, 0, "")

	res.Roles, err = r.getDbRoles(namespace)
	if err != nil {
		return nil, err
	}

	// First, extract a list of already active mutations and check if they have to be downed because they're
	// either inexistant or their hash changed.
	if rows, err = db.Query(context.Background(), `select name, file, needs, meta_needs, meta, sql, meta from dmut.mutations WHERE namespace = $1`, namespace); err != nil {
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

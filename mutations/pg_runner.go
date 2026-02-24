package mutations

import (
	"context"
	"fmt"
	"log"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	au "github.com/logrusorgru/aurora"
	"github.com/pkg/errors"
)

var _ Runner = &PgRunner{}

type PgRunner struct {
	isTesting bool
	conn      *pgx.Conn
}

func (r *PgRunner) Close() error {
	return r.conn.Close(context.Background())
}

func (r *PgRunner) IsTesting() bool {
	return r.isTesting
}

func NewPgRunner(url string, isTesting bool) (*PgRunner, error) {
	log.Println("connecting to", url)
	conn, err := pgx.Connect(context.Background(), url)
	if err != nil {
		return nil, err
	}

	res := &PgRunner{conn: conn, isTesting: isTesting}
	if err := res.InstallDmut(); err != nil {
		return nil, wrapPgError(err)
	}
	return res, nil
}

func wrapPgError(err error) error {
	if pgerr, ok := err.(*pgconn.PgError); ok {
		var details = pgerr.Detail
		return fmt.Errorf("%s (original error: %w)", details, err)
	}
	return err
}

func (r *PgRunner) execStatements(stmts []string) error {
	for _, stmt := range stmts {
		if err := r.exec(stmt); err != nil {
			return wrapPgError(err)
		}
	}
	return nil
}

func (r *PgRunner) ApplyMutation(m *DbMutation) error {

	log.Println(au.BrightGreen("✓"), m.DisplayName())

	if err := r.execStatements(m.Up); err != nil {
		return errors.Wrap(err, "error applying mutation "+m.DisplayName())
	}

	return r.saveMutation(m)
}

func (r *PgRunner) UndoMutation(m *DbMutation) error {

	log.Println(au.BrightRed("🗑"), m.DisplayName())
	if err := r.execStatements(m.Down); err != nil {
		return err
	}
	return r.deleteMutation(m)
}

func (r *PgRunner) deleteMutation(m *DbMutation) error {
	_, err := r.conn.Exec(context.Background(), `DELETE FROM dmut.mutations WHERE hash = $1`, m.Hash)
	return wrapPgError(err)
}

func (r *PgRunner) saveMutation(m *DbMutation) error {
	_, err := r.conn.Exec(context.Background(), `INSERT INTO dmut.mutations(hash, name, meta, up, down, children, parents) VALUES ($1, $2, $3, $4, $5, $6, $7)`, m.Hash, m.Name, m.Meta, m.Up, m.Down, m.Children, m.Parents)
	return wrapPgError(err)
}

func (r *PgRunner) Commit() error {
	_, err := r.conn.Exec(context.Background(), `COMMIT`)
	return wrapPgError(err)
}

func (r *PgRunner) SavePoint(name string) error {
	if name == "" {
		if err := r.exec("BEGIN"); err != nil {
			return wrapPgError(err)
		}
		return nil
	}
	return r.exec(`SAVEPOINT ` + name)
}

func (r *PgRunner) RollbackToSavepoint(name string) error {
	var cmd = `ROLLBACK`
	if name != "" {
		cmd += ` TO SAVEPOINT ` + name
	}
	return r.exec(cmd)
}

func (r *PgRunner) exec(sql string) error {
	_, err := r.conn.Exec(context.Background(), sql)
	if err != nil {
		err = errors.Wrap(err, au.BrightRed("error in statement").String()+" "+au.BrightBlue(sql).String())
	}
	return wrapPgError(err)
}

func (r *PgRunner) getDbRoles() (mapset.Set[string], error) {
	var res = mapset.NewSet[string]()
	rows, err := r.conn.Query(context.Background(), `SELECT rolname FROM dmut.roles`)
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

func (r *PgRunner) ReconcileRoles(roles mapset.Set[string]) error {
	db_roles, err := r.getDbRoles()
	if err != nil {
		return err
	}
	missing_roles := roles.Difference(db_roles)
	leftover_roles := db_roles.Difference(roles)

	if leftover_roles.Cardinality() > 0 {

		// Drop all meta mutations before dropping the roles
		muts, err := r.GetDBMutationsFromDb()
		if err != nil {
			return err
		}
		for _, mut := range muts {
			if mut.Meta {
				if err := r.UndoMutation(mut); err != nil {
					return err
				}
			}
		}

		for _, role := range leftover_roles.ToSlice() {
			role := `"` + role + `"`
			log.Println(au.BrightRed("💀"), "dropping role", role)
			if err := r.exec(`DROP ROLE ` + role); err != nil {
				return err
			}
		}
	}

	if missing_roles.Cardinality() > 0 {
		for _, role := range missing_roles.ToSlice() {
			log.Println(au.BrightGreen("🗣"), "creating role", role)
			if err := r.exec(`CREATE ROLE "` + role + `"`); err != nil {
				return err
			}
		}
	}

	return nil
}

// get the mutations already in the database
func (r *PgRunner) GetDBMutationsFromDb() (DbMutationMap, error) {
	var (
		db      = r.conn
		res_map = make(DbMutationMap)
		rows    pgx.Rows
		err     error
	)

	// First, extract a list of already active mutations and check if they have to be downed because they're
	// either inexistant or their hash changed.
	if rows, err = db.Query(context.Background(), `select hash, name, meta, up, down, children, parents from dmut.mutations m`); err != nil {
		return nil, wrapPgError(err)
	}
	defer rows.Close()

	for rows.Next() {
		var dbmut DbMutation
		if err = rows.Scan(&dbmut.Hash, &dbmut.Name, &dbmut.Meta, &dbmut.Up, &dbmut.Down, &dbmut.Children, &dbmut.Parents); err != nil {
			return nil, wrapPgError(err)
		}
		res_map.AddMutation(&dbmut)
	}

	return res_map, nil
}

func (r *PgRunner) InstallDmut() error {
	_, err := r.conn.Exec(context.Background(), `
	CREATE SCHEMA IF NOT EXISTS dmut;
	CREATE TABLE IF NOT EXISTS dmut.mutations (
		hash TEXT PRIMARY KEY,
		name TEXT,
		meta BOOLEAN,
		up TEXT[],
		down TEXT[],
		children TEXT[],
		parents TEXT[],
		ts TIMESTAMP WITH TIME ZONE DEFAULT NOW()
	);
	CREATE TABLE IF NOT EXISTS dmut.roles (
		rolname TEXT PRIMARY KEY
	);
`)
	return wrapPgError(err)
}

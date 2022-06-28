package mutations

import (
	"fmt"

	"github.com/jackc/pgx/v4"
)

func testCompile() {
	// The only use of this function is to make sure we implement the interface correctly
	var tst = func(r Runner) {}
	tst(&PgRunner{})
}

type PgRunner struct {
	conn *pgx.Conn
}

func NewPgRunner(host string) (*PgRunner, error) {
	conn, err := pgx.Connect(ctx(), host)
	if err != nil {
		return nil, err
	}
	return &PgRunner{conn}, nil
}

func (r *PgRunner) SaveMutation(m *Mutation) error {
	if _, err := r.conn.Exec(
		ctx(),
		`INSERT INTO dmut.mutations(hash, name, up, down, children) VALUES ($1, $2, $3, $4, $5)`,
		m.Hash,
		m.Name,
		m.Up,
		m.Down,
		m.GetChildrenNames(),
	); err != nil {
		return fmt.Errorf("can't insert into mutations table %w", err)
	}
	return nil
}

func (r *PgRunner) DeleteMutation(name string) error {
	_, err := r.conn.Exec(ctx(), `DELETE FROM dmut.mutations WHERE name = $1`, name)
	return err
}

func (r *PgRunner) Commit() error {
	_, err := r.conn.Exec(ctx(), `COMMIT`)
	return err
}

func (r *PgRunner) SavePoint(name string) error {
	if name == "" {
		if err := r.Exec("BEGIN"); err != nil {
			return err
		}
		return nil
	}
	return r.Exec(`SAVEPOINT ` + name)
}

func (r *PgRunner) RollbackToSavepoint(name string) error {
	var cmd = `ROLLBACK`
	if name != "" {
		cmd += ` TO SAVEPOINT ` + name
	}
	return r.Exec(cmd)
}

func (r *PgRunner) Exec(sql string) error {
	_, err := r.conn.Exec(ctx(), sql)
	return err
}

// get the mutations already in the database
func (r *PgRunner) GetDBMutations() ([]*DbMutation, error) {
	var (
		db     = r.conn
		res    = make([]*DbMutation, 0)
		rows   pgx.Rows
		exists bool
		err    error
	)

	row := db.QueryRow(ctx(), `SELECT EXISTS (
		SELECT FROM information_schema.tables
		WHERE  table_schema = 'dmut'
		AND    table_name   = 'mutations'
		);
 `)
	if err = row.Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return res, nil
	}

	// First, extract a list of already active mutations and check if they have to be downed because they're
	// either inexistant or their hash changed.
	if rows, err = db.Query(ctx(), `select row_to_json(m) from dmut.mutations m order by date_applied`); err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var dbmut DbMutation
		if err = rows.Scan(&dbmut); err != nil {
			return nil, err
		}
		res = append(res, &dbmut)
	}

	return res, nil
}

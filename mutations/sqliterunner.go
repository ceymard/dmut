package mutations

import (
	"database/sql"
	"encoding/json"
	"fmt"

	sqlite3 "github.com/mattn/go-sqlite3"
)

func toJsonArray(s []string) string {
	res, _ := json.Marshal(s)
	return string(res)
}

func fromJsonArray(s string) []string {
	var res []string
	_ = json.Unmarshal([]byte(s), &res)
	return res
}

func testCompileSqlite() {
	// The only use of this function is to make sure we implement the interface correctly
	var tst = func(r Runner) {}
	tst(&SqliteRunner{})
}

type SqliteRunner struct {
	db *sql.DB
}

func sqliteDriver() {
	sql.Register("sqlite3-custom", &sqlite3.SQLiteDriver{})
}

func NewSqliteRunner(filename string) (*SqliteRunner, error) {
	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		return nil, err
	}
	return &SqliteRunner{db}, nil
}

func (r *SqliteRunner) SaveMutation(m *Mutation) error {
	// FIXME marshal children names
	if _, err := r.db.Exec(
		`INSERT INTO _dmut_mutations(hash, name, up, down, children) VALUES ($1, $2, $3, $4, $5)`,
		m.Hash,
		m.Name,
		toJsonArray(m.Up),
		toJsonArray(m.Down),
		toJsonArray(m.GetChildrenNames()),
	); err != nil {
		return fmt.Errorf("can't insert into mutations table %w", err)
	}
	return nil
}

func (r *SqliteRunner) DeleteMutation(name string) error {
	_, err := r.db.Exec(`DELETE FROM _dmut_mutations WHERE name = $1`, name)
	return err
}

func (r *SqliteRunner) Commit() error {
	_, err := r.db.Exec(`COMMIT`)
	return err
}

func (r *SqliteRunner) SavePoint(name string) error {
	return r.Exec(`SAVEPOINT ` + name)
}

func (r *SqliteRunner) RollbackToSavepoint(name string) error {
	var cmd = `ROLLBACK`
	if name != "" {
		cmd += ` TO SAVEPOINT ` + name
	}
	return r.Exec(cmd)
}

func (r *SqliteRunner) Exec(sql string) error {
	_, err := r.db.Exec(sql)
	return err
}

// get the mutations already in the database
func (r *SqliteRunner) GetDBMutations() ([]*DbMutation, error) {
	var (
		db     = r.db
		res    = make([]*DbMutation, 0)
		rows   *sql.Rows
		exists bool
		err    error
	)

	row := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='_dmut_mutations';`)
	if err = row.Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return res, nil
	}

	// First, extract a list of already active mutations and check if they have to be downed because they're
	// either inexistant or their hash changed.
	if rows, err = db.Query(`
		select
			json_object(
				'hash', hash,
				'name', name,
				'up', up,
				'down', down,
				'children', children
			) as json
		from _dmut_mutations m order by date_applied`); err != nil {
		return nil, err
	}
	defer rows.Close()

	type intermediaryMut struct {
		Hash     string `json:"hash"`
		Name     string `json:"name"`
		Up       string `json:"up"`
		Down     string `json:"down"`
		Children string `json:"children"`
	}

	for rows.Next() {
		var inter intermediaryMut
		var jint string
		var dbmut DbMutation
		// besoin peut être d'une string pour lire le json avant
		if err = rows.Scan(&jint); err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(jint), &inter); err != nil {
			return nil, err
		}
		dbmut.Name = inter.Name
		dbmut.Hash = inter.Hash
		dbmut.Children = fromJsonArray(inter.Children)
		dbmut.Up = fromJsonArray(inter.Up)
		dbmut.Down = fromJsonArray(inter.Down)
		res = append(res, &dbmut)
	}

	return res, nil
}

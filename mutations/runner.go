package mutations

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/logrusorgru/aurora"
)

var (
	au = aurora.NewAurora(true)
)

// TestMutations test the mutations by doing, for each mutation in the set ;
//   remove them and apply the removal, check that there are no errors by doing so...
func TestMutations() {

}

func execCheck(db *pgx.Conn, sql string) error {
	_, err := db.Exec(ctx(), sql)
	if err != nil {
		log.Printf("error in statement: %s\n", au.Gray(12-1, sql))
		return err
	}
	return nil
}

func ctx() context.Context {
	return context.Background()
}

func MutationsWithout(muts Mutations, without string) Mutations {
	var (
		res Mutations
		mp  = make(map[string]struct{})
		tag func(*Mutation)
	)

	// tag the children that will have to go
	tag = func(m *Mutation) {
		mp[m.Name] = struct{}{}
		for _, c := range m.Children {
			tag(c)
		}
	}

	// find the mutation we need to oust, and mark it and its children as having to go.
	for _, m := range muts {
		if m.Name == without {
			tag(m)
			break
		}
	}

	// rebuild the slice without the previous mutations
	for _, m := range muts {
		if _, is_skipped := mp[m.Name]; !is_skipped {
			res = append(res, m)
		}
	}

	return res
}

type DbMutation struct {
	Hash       string
	Identifier string
	Up         []string
	Down       []string
	Children   []string
}

// get the mutations already in the database
func getDbMutations(db *pgx.Conn) ([]*DbMutation, error) {
	var (
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

// Compute what mutations should be downed from the database and which of the new set do actually have to be up.
func computeMutationDifference(dbmuts []*DbMutation, newmuts []*Mutation) (to_down []*DbMutation, to_up []*Mutation) {

	var (
		dbmap    = make(map[string]*DbMutation)
		localmap = make(map[string]*Mutation)
		down     func(m *DbMutation)
	)

	down = func(m *DbMutation) {
		if _, ok := dbmap[m.Identifier]; !ok {
			return
		}
		delete(dbmap, m.Identifier)
		var back = to_down
		to_down = []*DbMutation{m}
		to_down = append(to_down, back...)
		for _, c := range m.Children {
			if _, ok := dbmap[m.Identifier]; ok {
				down(dbmap[c])
			}
		}
	}

	////
	for _, dm := range dbmuts {
		dbmap[dm.Identifier] = dm
	}

	for _, mut := range newmuts {
		localmap[mut.Name] = mut
	}

	to_down = make([]*DbMutation, 0)
	to_up = make([]*Mutation, 0)

	///

	// for all the mutations already in database, figure out if they're still in the to_up list
	// and if they are, whether the hash has changed.
	for _, dm := range dbmuts {
		if corres, ok := localmap[dm.Identifier]; !ok || hex.EncodeToString(corres.Hash()) != dm.Hash {
			// this mutation will have to be removed. whatever is in the to_up will have to go
			// log.Print(dm.Identifier, ": ", string(hex.EncodeToString(corres.Hash())))
			down(dm)
		} else {
			// if it is still valid, remove it from the local map, as it shouldn't be processed.
			delete(localmap, dm.Identifier)
		}
	}

	for _, tu := range newmuts {
		if localmap[tu.Name] != nil {
			to_up = append(to_up, tu)
		}
	}

	// !!
	return to_down, to_up
}

// RunMutations runs the mutations in the database
//
//
func runMutations(db *pgx.Conn, mutations Mutations, testing bool) error {
	var (
		dbmuts []*DbMutation
		err    error
	)

	if db, err = pgx.Connect(ctx(), "postgres://app:app@2009-bms-engagement_postgres_1.docker/app?sslmode=disable"); err != nil {
		return err
	}

	// Start by downing the mutations that should be removed before being re-applied.
	if dbmuts, err = getDbMutations(db); err != nil {
		return err
	}

	to_down, to_up := computeMutationDifference(dbmuts, mutations)

	// Down the mutations that have to go
	for _, to_d := range to_down {
		_, _ = fmt.Print(au.Bold(au.Red(" < ")), to_d.Identifier, "\n")

		for _, down := range to_d.Down {
			if err = execCheck(db, down); err != nil {
				return fmt.Errorf("while downing mutation %s : %w", to_d.Identifier, err)
			}
		}

		if _, err = db.Exec(ctx(), `DELETE FROM dmut.mutations WHERE identifier = $1`, to_d.Identifier); err != nil {
			return fmt.Errorf("while downing mutation %s : %w", to_d.Identifier, err)
		}
	}

	// Now run the mutations that should be ran.
	for _, m := range to_up {
		for _, up := range m.Up {
			if err = execCheck(db, up); err != nil {
				return fmt.Errorf("while running mutation %s : %w", m.Name, err)
			}
		}

		// If we got here, insert into dmut base table
		if _, err = db.Exec(
			ctx(),
			`INSERT INTO dmut.mutations(hash, identifier, up, down, children) VALUES ($1, $2, $3, $4, $5)`,
			hex.EncodeToString(m.Hash()),
			m.Name,
			m.Up,
			m.Down,
			m.GetChildrenNames(),
		); err != nil {
			return fmt.Errorf("can't insert into mutations table %w", err)
		}

		_, _ = fmt.Printf(" %s %s\n", au.Green(`*`), m.Name)
	}

	// Testing should be done on *all* the mutations that are present, to check for faulty logic

	return nil
}

func RunMutations(muts Mutations) error {
	var (
		db  *pgx.Conn
		err error
	)

	if db, err = pgx.Connect(ctx(), "postgres://app:app@2009-bms-engagement_postgres_1.docker/app?sslmode=disable"); err != nil {
		return err
	}

	db.Exec(ctx(), `BEGIN`)
	defer func() {
		if err != nil {
			db.Exec(ctx(), `ROLLBACK`)
		} else {
			log.Print("committing changes")
			db.Exec(ctx(), `COMMIT`)
		}
	}()

	if err = runMutations(db, muts, false); err != nil {
		return err
	}

	log.Print("Now testing mutations")
	for _, m := range muts {
		log.Print("Testing ", m.Name)
		if strings.HasPrefix(m.Name, "dmut.") {
			continue
		}

		var testm = MutationsWithout(muts, m.Name)
		db.Exec(ctx(), `SAVEPOINT testdmut`)
		if err = runMutations(db, testm, true); err != nil {
			return err
		}
		// Also re-run the mutations right after
		if err = runMutations(db, muts, true); err != nil {
			return err
		}
		db.Exec(ctx(), `ROLLBACK TO SAVEPOINT testdmut`)
	}

	return err
}

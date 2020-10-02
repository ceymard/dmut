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
		return fmt.Errorf("error in statement: %s %w", au.Gray(12-1, sql), err)
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
	Hash     string
	Name     string
	Up       []string
	Down     []string
	Children []string
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
		if _, ok := dbmap[m.Name]; !ok {
			return
		}
		delete(dbmap, m.Name)
		var back = to_down
		to_down = []*DbMutation{m}
		to_down = append(to_down, back...)
		for _, c := range m.Children {
			if _, ok := dbmap[m.Name]; ok {
				down(dbmap[c])
			}
		}
	}

	////
	for _, dm := range dbmuts {
		dbmap[dm.Name] = dm
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
		if corres, ok := localmap[dm.Name]; !ok || hex.EncodeToString(corres.Hash()) != dm.Hash {
			// this mutation will have to be removed. whatever is in the to_up will have to go
			// log.Print(dm.Identifier, ": ", string(hex.EncodeToString(corres.Hash())))
			down(dm)
		} else {
			// if it is still valid, remove it from the local map, as it shouldn't be processed.
			delete(localmap, dm.Name)
		}
	}

	for _, tu := range newmuts {
		if localmap[tu.Name] != nil {
			to_up = append(to_up, tu)
		}
	}

	// !!
	return reorderDbMutations(to_down), to_up
}

// RunMutations runs the mutations in the database
//
//
func runMutations(db *pgx.Conn, mutations Mutations, testing bool) (bool, error) {
	var (
		dbmuts []*DbMutation
		err    error
	)

	// Start by downing the mutations that should be removed before being re-applied.
	if dbmuts, err = getDbMutations(db); err != nil {
		return false, fmt.Errorf("error when getting mutations: %w", err)
	}

	to_down, to_up := computeMutationDifference(dbmuts, mutations)

	if len(to_down) == 0 && len(to_up) == 0 {
		return false, nil
	}

	// Down the mutations that have to go
	for _, to_d := range to_down {
		if !testing {
			_, _ = fmt.Print(au.Bold(au.Red(" < ")), to_d.Name, "\n")
		}

		for _, down := range to_d.Down {
			if err = execCheck(db, down); err != nil {
				return false, fmt.Errorf("while downing mutation %s : %w", to_d.Name, err)
			}
		}

		if _, err = db.Exec(ctx(), `DELETE FROM dmut.mutations WHERE name = $1`, to_d.Name); err != nil {
			return false, fmt.Errorf("while downing mutation %s : %w", to_d.Name, err)
		}
	}

	// Now run the mutations that should be ran.
	for _, m := range to_up {
		if !testing {
			_, _ = fmt.Printf(" %s %s\n", au.Green(`*`), m.Name)
		}

		for _, up := range m.Up {
			if err = execCheck(db, up); err != nil {
				return false, fmt.Errorf("while running mutation %s : %w", m.Name, err)
			}
		}

		// If we got here, insert into dmut base table
		if _, err = db.Exec(
			ctx(),
			`INSERT INTO dmut.mutations(hash, name, up, down, children) VALUES ($1, $2, $3, $4, $5)`,
			hex.EncodeToString(m.Hash()),
			m.Name,
			m.Up,
			m.Down,
			m.GetChildrenNames(),
		); err != nil {
			return false, fmt.Errorf("can't insert into mutations table %w", err)
		}
	}

	// Testing should be done on *all* the mutations that are present, to check for faulty logic

	return true, nil
}

func RunMutations(host string, muts Mutations) error {
	var (
		db          *pgx.Conn
		err         error
		should_test bool
	)

	if db, err = pgx.Connect(ctx(), host); err != nil {
		return err
	}

	db.Begin(ctx())
	defer func() {
		if err != nil {
			log.Print(`Rollbacking and canceling`)
			db.Exec(ctx(), `ROLLBACK`)
		} else {
			log.Print("committing changes")
			db.Exec(ctx(), `COMMIT`)
		}
	}()

	if should_test, err = runMutations(db, muts, false); err != nil {
		return err
	}

	if !should_test {
		return nil
	}

	// log.Print("Now testing mutations")
	for _, m := range muts {
		log.Print("Testing ", m.Name)
		if strings.HasPrefix(m.Name, "dmut.") {
			continue
		}

		var testm = MutationsWithout(muts, m.Name)
		db.Exec(ctx(), `SAVEPOINT testdmut`)
		// Test removing the mutation
		// This is incorrect ; mutation removal should be tested in all orders...
		_, err = runMutations(db, testm, false)
		if err == nil {
			// Now test re-running it
			_, err = runMutations(db, muts, true)
		}
		db.Exec(ctx(), `ROLLBACK TO SAVEPOINT testdmut`)
		if err != nil {
			err = fmt.Errorf("while testing mutations: %w", err)
			return err
		}
	}

	return err
}

func reorderDbMutations(muts []*DbMutation) []*DbMutation {
	var (
		all_muts = make(map[string]*DbMutation)
		mp       = make(map[string]*DbMutation)
		res      = make([]*DbMutation, 0, len(muts))
		add      func(m *DbMutation)
	)

	for _, m := range muts {
		all_muts[m.Name] = m
	}

	add = func(m *DbMutation) {
		// do not process a mutation that already was added
		if _, ok := mp[m.Name]; ok {
			return
		}
		for _, p := range m.Children {
			var thedbmut = all_muts[p]
			if thedbmut != nil {
				add(thedbmut)
			}
		}
		res = append(res, m)
	}

	for _, m := range muts {
		add(m)
	}

	return res
}

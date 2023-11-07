package mutations

import (
	"context"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/logrusorgru/aurora"
)

var (
	au = aurora.NewAurora(true)
)

type Runner interface {
	GetDBMutations() ([]*DbMutation, error)
	SavePoint(name string) error
	// if name == "" then it's the total rollback
	RollbackToSavepoint(name string) error
	Commit() error
	DeleteMutation(name string) error
	SaveMutation(m *Mutation) error
	Exec(sql string) error
}

func execCheck(runner Runner, sql string) error {
	if err := runner.Exec(sql); err != nil {
		return fmt.Errorf("error in statement: %s %w", au.Gray(12, sql), err)
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
		if corres, ok := localmap[dm.Name]; !ok || corres.Hash != dm.Hash {
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
func runMutations(runner Runner, mutations Mutations, testing bool) (bool, error) {
	var (
		dbmuts   []*DbMutation
		err      error
		show_all = os.Getenv("DMUT_SHOW_ALL")
	)

	// Start by downing the mutations that should be removed before being re-applied.
	if dbmuts, err = runner.GetDBMutations(); err != nil {
		return false, fmt.Errorf("error when getting mutations: %w", err)
	}

	to_down, to_up := computeMutationDifference(dbmuts, mutations)

	if len(to_down) == 0 && len(to_up) == 0 {
		return false, nil
	}

	// Down the mutations that have to go
	for _, to_d := range to_down {
		if show_all != "" || !testing {
			_, _ = fmt.Print(au.Bold(au.Red(" < ")), to_d.Name, "\n")
		}

		for _, down := range to_d.Down {
			// fmt.Print(au.Gray(4, down), "\n")
			if err = execCheck(runner, down); err != nil {
				return false, fmt.Errorf("while downing mutation %s : %w", to_d.Name, err)
			}
		}

		if err = runner.DeleteMutation(to_d.Name); err != nil {
			return false, fmt.Errorf("while removing mutation from dmut %s : %w", to_d.Name, err)
		}
	}

	// Now run the mutations that should be ran.
	for _, m := range to_up {
		if show_all != "" || !testing {
			_, _ = fmt.Printf(" %s %s\n", au.Green(`*`), m.Name)
		}

		for _, up := range m.Up {
			if err = execCheck(runner, up); err != nil {
				return false, fmt.Errorf("while running mutation %s in '%s': %w", m.Name, m.File, err)
			}
		}

		// If we got here, insert into dmut base table
		if err = runner.SaveMutation(m); err != nil {
			return false, err
		}
	}

	// Testing should be done on *all* the mutations that are present, to check for faulty logic

	return true, nil
}

func RunMutations(runner Runner, muts Mutations) error {
	var (
		// db          *pgx.Conn
		err         error
		should_test bool
	)

	if err := runner.SavePoint(""); err != nil {
		return err
	}
	// db.Begin(ctx())
	defer func() {
		if err != nil {
			log.Print(`Rollbacking and canceling`)
			runner.RollbackToSavepoint("")
			// db.Exec(ctx(), `ROLLBACK`)
		} else {
			if len(muts) > 0 && should_test {
				log.Print("dmut: committing changes ")
				runner.Commit()
			} else {
				log.Print("dmut: no changes, not doing anything")
			}
		}
	}()

	if should_test, err = runMutations(runner, muts, false); err != nil {
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
		runner.SavePoint("testdmut")
		// Test removing the mutation
		// This is incorrect ; mutation removal should be tested in all orders...
		_, err = runMutations(runner, testm, true)
		if err == nil {
			// Now test re-running it
			_, err = runMutations(runner, muts, true)
		}
		runner.RollbackToSavepoint("testdmut")
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
		mp[m.Name] = m
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

var reIsPostgres = regexp.MustCompilePOSIX(`^(postgres|pg)://`)

func isPgUrl(host string) bool {
	return true
}

// ParseAndRunMutations parses mutations from one or several files
// and attemts to run them on a host
func ParseAndRunMutations(host string, files ...string) error {
	// expr := &TopLevel{}
	var (
		runner Runner
	)

	for _, filename := range files {

		mp, err := GetMutationMapFromFile(filename)
		if err != nil {
			return err
			// continue
		}

		var mutsOrig Mutations = (*mp).GetInOrder()
		var muts []*Mutation
		if reIsPostgres.Match([]byte(host)) {
			muts = append([]*Mutation{}, DmutMutations...)
			if runner, err = NewPgRunner(host); err != nil {
				return err
			}
		} else {
			muts = append([]*Mutation{}, DmutSqliteMutations...)
			if runner, err = NewSqliteRunner(host); err != nil {
				return err
			}
		}
		muts = append(muts, mutsOrig...)

		// now that we have
		if err = RunMutations(runner, muts); err != nil {
			return err
		}
	}

	return nil
}

package mutations

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"

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
		log.Printf("In statement: %s\n", au.Gray(12-1, sql))
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

func RunMutations(mutations Mutations) error {
	db, err := pgx.Connect(ctx(), "postgres://app:app@2009-bms-engagement_postgres_1.docker/app?sslmode=disable")
	if err != nil {
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

	for _, m := range mutations {
		for _, up := range m.Up {
			if err = execCheck(db, up); err != nil {
				return fmt.Errorf("while running mutation %s : %w", m.Name, err)
			}
		}

		// If we got here, insert into dmut base table
		if _, err = db.Exec(
			ctx(),
			`INSERT INTO dmut.mutations(hash, identifier, up, down, parents) VALUES ($1, $2, $3, $4, $5)`,
			hex.EncodeToString(m.Hash()),
			m.Name,
			m.Up,
			m.Down,
			m.GetParentNames(),
		); err != nil {
			return fmt.Errorf("can't insert into mutations table %w", err)
		}

		_, _ = fmt.Printf(" %s %s\n", au.Green(`*`), m.Name)
	}

	return nil
}

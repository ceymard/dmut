package mutations

import "iter"

type Runnable struct {
	Mutation  *Mutation
	Direction IterationDirection
}

func (r *Runnable) Statements() iter.Seq[string] {
	var stmts []MutationStatement
	if r.Direction.Down {
		stmts = r.Mutation.Sql
	} else {
		stmts = r.Mutation.Meta
	}
	return func(yield func(string) bool) {
		for _, stmt := range stmts {
			if r.Direction.Down {
				yield(stmt.Down)
			} else {
				yield(stmt.Up)
			}
		}
	}
}

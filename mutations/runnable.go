package mutations

import (
	"iter"

	"github.com/ugurcsen/gods-generic/maps/linkedhashmap"
)

type Runnable struct {
	Mutation  *Mutation
	Direction IterationDirection
}

func (r *Runnable) Statements() iter.Seq2[int, string] {
	var stmts []MutationStatement
	if r.Direction.Meta {
		stmts = r.Mutation.Meta
	} else {
		stmts = r.Mutation.Sql
	}

	return func(yield func(int, string) bool) {
		if r.Direction.Down {
			// yield them in reverse order
			for i := len(stmts) - 1; i >= 0; i-- {
				if !yield(i, stmts[i].Down) {
					return
				}
			}
		} else {
			for i, stmt := range stmts {
				if !yield(i, stmt.Up) {
					return
				}
			}
		}
	}
}

type RunnableMap struct {
	*linkedhashmap.Map[string, *Runnable]
}

func NewRunnableMap() *RunnableMap {
	return &RunnableMap{
		Map: linkedhashmap.New[string, *Runnable](),
	}
}

func (rm *RunnableMap) Run(runner Executor) error {
	for _, runnable := range rm.Values() {
		if err := runner.Run(runnable); err != nil {
			return err
		}
	}
	return nil
}

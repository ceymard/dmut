package mutations

import (
	"iter"

	"github.com/ugurcsen/gods-generic/maps/linkedhashmap"
)

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
				if !yield(stmt.Down) {
					return
				}
			} else {
				if !yield(stmt.Up) {
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

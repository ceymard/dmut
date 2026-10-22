package mutations

import (
	"context"
	"iter"

	"github.com/ugurcsen/gods-generic/maps/linkedhashmap"
)

type Runnable struct {
	Mutation  *Mutation
	Direction IterationDirection
}

func (r *Runnable) IsEmpty() bool {
	return r.Size() == 0
}

// LogAttrs returns the slog attributes identifying this runnable.
func (r *Runnable) LogAttrs() []any {
	return append(r.Mutation.LogAttrs(), "dir", r.Direction.UpOrDown(), "kind", r.Direction.MetaOrSql())
}

func (r *Runnable) Size() int {
	if r.Direction.Meta {
		return len(r.Mutation.Glue)
	} else {
		return len(r.Mutation.Core)
	}
}

func (r *Runnable) Statements() iter.Seq2[int, string] {
	var stmts []MutationStatement
	if r.Direction.Meta {
		stmts = r.Mutation.Glue
	} else {
		stmts = r.Mutation.Core
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

func (rm *RunnableMap) Run(ctx context.Context, runner Executor) error {
	for _, runnable := range rm.Values() {
		if err := runner.Run(ctx, runnable); err != nil {
			return err
		}
	}
	return nil
}

// Counts returns how many runnables of rm run at least one statement, and how
// many statements they run in all.
func (rm *RunnableMap) Counts() (mutations, statements int) {
	for _, runnable := range rm.Values() {
		if n := runnable.Size(); n > 0 {
			mutations++
			statements += n
		}
	}
	return mutations, statements
}

package mutations

import (
	"fmt"
	"iter"

	au "github.com/logrusorgru/aurora"
	"github.com/ugurcsen/gods-generic/maps/linkedhashmap"
)

type Runnable struct {
	Mutation  *Mutation
	Direction IterationDirection
}

func (r *Runnable) IsEmpty() bool {
	return r.Size() == 0
}

func (r *Runnable) DisplayName() string {
	up_or_down := au.BrightGreen("↑").String()
	if r.Direction.Down {
		up_or_down = au.BrightRed("↓").String()
	}
	meta_or_sql := au.BrightGreen("sql").String()
	if r.Direction.Meta {
		meta_or_sql = au.BrightCyan("meta").String()
	}
	return fmt.Sprintf("%s %s.%s", up_or_down, r.Mutation.DisplayName(), meta_or_sql)
}

func (r *Runnable) Size() int {
	if r.Direction.Meta {
		return len(r.Mutation.Meta)
	} else {
		return len(r.Mutation.Sql)
	}
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

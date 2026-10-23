package mutations

import (
	"context"
	"fmt"
	"slices"

	"github.com/samber/oops"
)

// Test a mutation set by running all mutations independently, and resetting after each one.
// Consider that the set is already up in the database.
func TestMutationSet(ctx context.Context, runner Executor, set *MutationSet) (err error) {

	runner.BeginCapture(Capture{Phase: "test"})
	defer func() { runner.EndCapture(err != nil, "") }()

	if err = runner.SavePoint(ctx, "test_mutation_set"); err != nil {
		return err
	}

	// The set is fully up here, which is the only moment glue edits can be tried.
	if err = TestGlueEdits(ctx, runner, set); err != nil {
		return err
	}

	// Downing
	var fake_empty_local_set *MutationSet = nil
	sql_down, _ := fake_empty_local_set.GetMutationsDelta(set, ITER_SQL)
	meta_down, _ := fake_empty_local_set.GetMutationsDelta(set, ITER_META)

	// Down the meta to be able to test it independently
	if err = meta_down.Run(ctx, runner); err != nil {
		return err
	}

	// Core up, glue down : what a core edit finds once apply has downed all the glue.
	if err = TestCoreEdits(ctx, runner, set); err != nil {
		return err
	}

	// Test the meta
	if err = MutationTestSequence(ctx, runner, set, ITER_META); err != nil {
		return err
	}

	runner.Logger().Info("downing sql", "count", sql_down.Size())

	// Then, down the SQL
	if err = sql_down.Run(ctx, runner); err != nil {
		return err
	}

	// Test the SQL
	if err = MutationTestSequence(ctx, runner, set, ITER_SQL); err != nil {
		return err
	}

	if err = runner.RollbackToSavepoint(ctx, "test_mutation_set"); err != nil {
		runner.Logger().Error("error rolling back to savepoint", "error", err)
	}

	if err = runner.ReleaseSavepoint(ctx, "test_mutation_set"); err != nil {
		runner.Logger().Error("error releasing savepoint", "error", err)
	}

	return nil

}

// With the test runner, try to up all mutations independently, and reset after each one.
func MutationTestSequence(ctx context.Context, runner Executor, set *MutationSet, dir IterationDirection) error {

	if err := runner.SavePoint(ctx, "independent_test"); err != nil {
		return err
	}

	// Test all mutations independently
	for mutation := range set.AllMutations() {
		runner.Logger().Info("testing", append(mutation.LogAttrs(), "kind", dir.MetaOrSql())...)

		var inner_mutations []*Mutation
		for mut := range mutation.IterateDependencies(dir) {
			inner_mutations = append(inner_mutations, mut)
		}

		for _, mut := range inner_mutations {
			if err := runner.Run(ctx, mut.Runnable(dir)); err != nil {
				return err
			}
		}

		down_dir := dir
		down_dir.Down = true
		slices.Reverse(inner_mutations)
		for _, mut := range inner_mutations {
			if err := runner.Run(ctx, mut.Runnable(down_dir)); err != nil {
				return err
			}
		}

		if err := runner.RollbackToSavepoint(ctx, "independent_test"); err != nil {
			return err
		}
	}

	if err := runner.ReleaseSavepoint(ctx, "independent_test"); err != nil {
		return err
	}

	return nil
}

// TestGlueEdits checks that editing any single glue statement of any mutation can be applied.
//
// The whole set must be up in the database. For every glue statement of every mutation it
// runs the glue changes that apply would compute if that one statement were edited, down
// then up, and rolls them back. It fails when one of them cannot run, naming the mutation
// and the statement. A typical cause is a mutation using what another one creates without
// listing it in glue_needs: it is then not downed with it, and Postgres refuses to drop
// what it still uses. It costs a down and up of the glue after each statement, plus that of
// what depends on them, and leaves the database as it found it.
func TestGlueEdits(ctx context.Context, runner Executor, set *MutationSet) error {
	return testEdits(ctx, runner, set, ITER_META)
}

// TestCoreEdits checks that editing any single core statement of any mutation can be applied,
// the way apply would after downing all the glue.
//
// The core of the whole set must be up and all the glue down. For every core statement of every
// mutation it runs the core changes that apply would compute if that one statement were edited,
// and rolls them back. Default mode is expected to be blocked by a dependent mutation (a table
// referencing the edited one), so when that happens the rebuild of the mutation and everything
// depending on it that `full` mode would do is tried instead. It fails when even that cannot
// run, naming the mutation and the statement. A typical cause is a mutation using what another
// one creates without listing it in needs. Nothing is kept.
func TestCoreEdits(ctx context.Context, runner Executor, set *MutationSet) error {
	return testEdits(ctx, runner, set, ITER_SQL)
}

// testEdits is TestGlueEdits and TestCoreEdits : dir tells which half of the mutations is edited.
func testEdits(ctx context.Context, runner Executor, set *MutationSet, dir IterationDirection) error {
	names := slices.Sorted(slices.Values(set.Keys()))

	total := 0
	for _, name := range names {
		mut, _ := set.GetMutation(name)
		total += len(statementsOfDir(mut, dir))
	}
	runner.Logger().Info("testing "+dir.MetaOrSql()+" edits", "count", total)

	var needs_full []string
	for _, name := range names {
		mut, _ := set.GetMutation(name)
		for index := range statementsOfDir(mut, dir) {
			blocked, err := testEdit(ctx, runner, set, mut, index, dir)
			if err != nil {
				return err
			}
			if blocked {
				needs_full = append(needs_full, fmt.Sprintf("%s#%d", name, index))
			}
		}
	}
	if len(needs_full) > 0 {
		runner.Logger().Debug("core edits that need full mode", "statements", needs_full)
	}
	return nil
}

func statementsOfDir(mut *Mutation, dir IterationDirection) []MutationStatement {
	if dir.Meta {
		return mut.Glue
	}
	return mut.Core
}

// testEdit runs the changes of editing one statement and rolls them back. blocked reports that
// default mode could not apply a core edit, which only full mode can.
func testEdit(ctx context.Context, runner Executor, set *MutationSet, mut *Mutation, index int, dir IterationDirection) (blocked bool, err error) {
	edited := set.withEdited(mut.Name, index, dir)
	down, up, _ := edited.GetLCPDelta(set, dir)

	if err := runner.SavePoint(ctx, "edit_test"); err != nil {
		return false, err
	}

	run := func(down, up *RunnableMap) error {
		if err := down.Run(ctx, runner); err != nil {
			return err
		}
		return up.Run(ctx, runner)
	}
	attempt_err := run(down, up)
	if attempt_err != nil && !dir.Meta {
		// A blocked tail is expected for core, see TestCoreEdits : rebuild what depends on it.
		if err := runner.RollbackToSavepoint(ctx, "edit_test"); err != nil {
			return false, err
		}
		blocked = true
		expanded_down, expanded_up := edited.ExpandForFullRebuild(down.Keys(), set, dir)
		attempt_err = run(expanded_down, expanded_up)
	}

	if err := runner.RollbackToSavepoint(ctx, "edit_test"); err != nil {
		return false, err
	}
	if err := runner.ReleaseSavepoint(ctx, "edit_test"); err != nil {
		return false, err
	}
	if attempt_err != nil {
		hint := "if postgres says other objects depend on something, the mutation using it is missing from needs"
		if dir.Meta {
			hint = "if postgres says other objects depend on something, the mutation using it is missing from glue_needs"
		}
		return blocked, oops.In("mutations").
			With("mutation", mut.Name).
			With("statement_index", index).
			With("statement", statementsOfDir(mut, dir)[index].Up).
			Wrapf(attempt_err, "editing %s statement %d of %s could not be applied : %s", dir.MetaOrSql(), index, mut.Name, hint)
	}
	return blocked, nil
}

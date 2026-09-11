package mutations

import (
	"io"

	au "github.com/logrusorgru/aurora"
	"github.com/samber/oops"
	"github.com/ugurcsen/gods-generic/sets/hashset"
)

type MutationRunnerOptions struct {
	Verbose  bool
	Commit   bool
	Override bool
	All      bool

	// Full and Simple select dmut apply's execution mode (see specs/changes.md,
	// "Apply changes"). Neither set is the default mode: down the old segment and
	// up the new one directly, but don't pursue dependent downing if a statement
	// fails. They're mutually exclusive - callers (see command_apply.go) validate
	// that before constructing these options.
	Full   bool
	Simple bool

	// Output, when set, replaces the PgRunner's default os.Stdout as the
	// destination for dmut's own progress/notice log lines — lets an
	// embedding application (anything driving dmut as a library rather
	// than through the CLI) capture that output into its own logging
	// instead of it going straight to stdout. nil (the default) leaves
	// PgRunner's stdout behaviour unchanged.
	Output io.Writer
}

// Merge ORs every boolean field: a flag becomes true if any of the merged
// options request it, and nothing can turn a flag back off. That's
// intentional for opt-in switches (Verbose, Override, All) where several
// independent sources should each be able to request the behaviour without
// knowing about each other. Output isn't boolean, so it merges by
// last-write-wins instead: the last non-nil Output among others replaces
// whatever was set before it.
func (o *MutationRunnerOptions) Merge(others ...*MutationRunnerOptions) {
	for _, other := range others {
		o.Verbose = o.Verbose || other.Verbose
		o.Commit = o.Commit || other.Commit
		o.Override = o.Override || other.Override
		o.All = o.All || other.All
		o.Full = o.Full || other.Full
		o.Simple = o.Simple || other.Simple
		if other.Output != nil {
			o.Output = other.Output
		}
	}
}

// RunMutations runs the mutations for a given local mutation set. A transaction should be started before calling this function.
func RunMutations(runner Executor, local *MutationSet, opts ...*MutationRunnerOptions) error {

	var options = MutationRunnerOptions{}
	options.Merge(opts...)

	// A revision's own mode: declaration (see specs/changes.md, "Squash revisions")
	// wins outright over whatever the CLI invocation asked for - this is how a
	// reviewed, committed revision carries destructive intent through an automated
	// pipeline that otherwise always runs -s.
	effective_full, effective_simple := options.Full, options.Simple
	switch local.Mode {
	case "full":
		effective_full, effective_simple = true, false
	case "simple":
		effective_full, effective_simple = false, true
	case "default":
		effective_full, effective_simple = false, false
	}

	var err error
	has_changes := true

	if !options.Override {

		var namespace = local.Namespace
		var distant *MutationSet

		if distant, err = runner.GetDBMutationsFromDb(namespace); err != nil {
			return err
		}

		sql_down, sql_up, core_any_real_down := local.GetLCPDelta(distant, ITER_SQL)

		if effective_simple && core_any_real_down {
			return oops.In("apply").
				With("namespace", local.Namespace).
				Errorf("refusing under simple mode: core has a change that isn't a pure append - rerun with -f, or without -s, if that's intended")
		}

		// If any core statement needed a real down (not just an append), all glue in
		// the namespace is downed and re-upped wholesale, exactly as before - only the
		// trigger condition changed (a real down, not merely "core differs at all").
		// Otherwise glue gets the same longest-common-prefix treatment as core, so an
		// isolated function/grant edit doesn't force a rebuild of unrelated glue.
		var meta_down, meta_up *RunnableMap
		if core_any_real_down {
			var fake_empty_local_set *MutationSet = nil
			_, meta_up = local.GetMutationsDelta(nil, ITER_META)
			meta_down, _ = fake_empty_local_set.GetMutationsDelta(distant, ITER_META)
		} else {
			meta_down, meta_up, _ = local.GetLCPDelta(distant, ITER_META)
		}

		has_changes = sql_up.Size() != 0 || meta_up.Size() != 0 || sql_down.Size() != 0 || meta_down.Size() != 0

		if !has_changes {
			// No changes, no tests !
			runner.Logger().Println(au.BrightGreen("≡"), "no changes to apply for namespace", au.BrightMagenta(local.Namespace).String(), "revision", au.BrightGreen(local.Revision).String())
		} else {
			runner.Logger().Println(au.BrightGreen("→"), "applying mutations for namespace", au.BrightMagenta(local.Namespace).String(), "revision", au.BrightGreen(local.Revision).String())

			// 1. Start by downing the meta
			if err := meta_down.Run(runner); err != nil {
				return err
			}

			if effective_full {
				// Try the tail-only attempt first, without downing dependants. If it
				// fails, roll back to before it and rebuild every mutation that needed
				// a down - and everything depending on it, leaves first - in full
				// rather than by tail, then retry once. No further retry: a second
				// failure aborts the whole apply exactly like default mode.
				if err := runner.SavePoint("core_attempt"); err != nil {
					return err
				}
				attempt_err := sql_down.Run(runner)
				if attempt_err == nil {
					attempt_err = sql_up.Run(runner)
				}
				if attempt_err != nil {
					if err := runner.RollbackToSavepoint("core_attempt"); err != nil {
						return err
					}
					expanded_down, expanded_up := local.ExpandForFullRebuild(sql_down.Keys(), distant, ITER_SQL)
					if err := expanded_down.Run(runner); err != nil {
						return err
					}
					if err := expanded_up.Run(runner); err != nil {
						return err
					}
				} else if err := runner.ReleaseSavepoint("core_attempt"); err != nil {
					return err
				}
			} else {
				if err := sql_down.Run(runner); err != nil {
					return err
				}

				if err := sql_up.Run(runner); err != nil {
					return err
				}
			}

			if err := meta_up.Run(runner); err != nil {
				return err
			}
		}

	}

	// delete: and override: are pure bookkeeping (see specs/changes.md, "Squash
	// revisions") - they apply regardless of options.Override, since neither one
	// executes anything either.
	if len(local.Delete) > 0 {
		if err := runner.DeleteMutations(local.Namespace, local.Delete); err != nil {
			return err
		}
	}

	// save_set is what actually gets recorded: local with any override: swapped in,
	// minus anything named in delete: - a mutation can be named in both (e.g. a child
	// being folded into its parent's override and retired from separate tracking in
	// the same revision), and delete: must win, or SaveMutations would immediately
	// resurrect the row DeleteMutations just removed. Always a fresh copy when either
	// is present, so local itself (the caller's own set) is never mutated in place.
	save_set := local
	if local.OverrideMutations != nil {
		save_set = local.WithOverrides()
	} else if len(local.Delete) > 0 {
		deleted := hashset.New[string]()
		for _, name := range local.Delete {
			deleted.Add(name)
		}
		save_set = NewMutationSet(local.Namespace, local.Revision, local.File)
		for mut := range local.AllMutations() {
			if !deleted.Contains(mut.Name) {
				save_set.AddMutation(mut)
			}
		}
	}

	if err := runner.SaveMutations(save_set); err != nil {
		return err
	}

	if has_changes {
		runner.Logger().Println(au.BrightGreen("🧪"), "performing tests")
		if err := TestMutationSet(runner, local); err != nil {
			return err
		}
	}

	if local.OverrideMutations != nil {
		// Test only what override: actually changed (its entries that survived
		// delete:, sourced from save_set so they carry the swapped-in content), not
		// the whole of save_set - TestMutationSet assumes everything it's given is
		// already fully up in the database, which delete: deliberately violates: it
		// untracks a mutation without downing its physical object, so testing the
		// rest of save_set (e.g. a parent schema untouched by override:) as one
		// block can spuriously fail on a delete:'d-but-still-physical sibling that
		// isn't even part of what's being verified here (a real bug this comment
		// used to not warn about - see the "delete + unrelated override" test).
		changed_set := NewMutationSet(local.Namespace, local.Revision, local.File)
		for override := range local.OverrideMutations.AllMutations() {
			if _, ok := save_set.GetMutation(override.Name); !ok {
				continue // delete: named this one too, and delete: wins
			}
			// Copy from the raw override entry, not save_set's already-resolved
			// copy: save_set's Needs may carry an auto-added implicit-parent name
			// (e.g. "api" for "api.audit_log") from being resolved inside the full
			// save_set - re-resolving that Needs value inside this narrower
			// changed_set fails ("asks for dependency api which was not found")
			// unless that parent happens to be in changed_set too. The raw
			// override entry's Needs is exactly what was declared, unresolved.
			cp := *override
			changed_set.AddMutation(&cp)
		}
		if changed_set.Size() > 0 {
			if err := changed_set.ResolveDependencies(); err != nil {
				return err
			}
			runner.Logger().Println(au.BrightGreen("🧪"), "performing tests with override")
			if err := TestMutationSet(runner, changed_set); err != nil {
				return err
			}
		}
	}
	runner.Logger().Println(au.BrightGreen("✓"), "tests passed")

	return nil
}

func RunAllMutations(runner Executor, namespaces *MutationNamespace, opts ...*MutationRunnerOptions) (err error) {

	var options = MutationRunnerOptions{}
	options.Merge(opts...)

	if err := runner.Begin(); err != nil {
		return err
	}

	// Every early return below is a failure path: leave the transaction open
	// only on the successful Commit/Rollback at the end, otherwise roll back
	// so a direct caller (not going through ReadAndRunMutations, which defers
	// runner.Close()) doesn't inherit a dangling open transaction.
	defer func() {
		if err != nil {
			runner.Rollback()
		}
	}()

	for _, namespace := range namespaces.Keys() {
		db_mutations, err := runner.GetDBMutationsFromDb(namespace)
		if err != nil {
			return err
		}

		revisions, ok := namespaces.Map.Get(namespace)
		if !ok {
			return oops.In("mutations").With("namespace", namespace).Errorf("no revision sequence found")
		}

		// Db has no mutations, only apply the highest local one
		if db_mutations.Revision == 0 {
			if !options.All {
				runner.Logger().Println(au.BrightGreen("→"), "no database mutations,applying highest local revision for namespace", namespace)
				if revision, ok := revisions.Revisions[revisions.MaxRevision]; ok {
					if err := RunMutations(runner, revision, opts...); err != nil {
						return err
					}
				}
				continue
			} else {
				// db revision cannot be less than 1
				db_mutations.Revision = 1
			}
		}

		for i := db_mutations.Revision; i <= revisions.MaxRevision; i++ {
			if revision, ok := revisions.Revisions[i]; ok {
				if err := RunMutations(runner, revision, opts...); err != nil {
					return err
				}
			}
		}
	}

	runner.Logger().Println(au.BrightGreen("🎉"), "no errors")
	if options.Commit {
		// runner.Logger().Println(au.BrightGreen("💾"), "committing")
		if err := runner.Commit(); err != nil {
			return err
		}
	} else {
		if err := runner.Rollback(); err != nil {
			return err
		}
	}

	return nil
}

func ReadAndRunMutations(uri string, paths []string, opts ...*MutationRunnerOptions) error {

	var options = MutationRunnerOptions{}
	options.Merge(opts...)

	muts, err := LoadYamlMutations(paths...)
	if err != nil {
		return err
	}

	runner, err := NewPgRunner(uri, options.Verbose)
	if err != nil {
		return err
	}
	defer runner.Close()

	if options.Output != nil {
		runner.SetOutput(options.Output)
	}

	// Test before
	if err := RunAllMutations(runner, muts, &options); err != nil {
		return err
	}

	return nil
}

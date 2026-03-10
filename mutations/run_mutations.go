package mutations

import (
	au "github.com/logrusorgru/aurora"
	"github.com/samber/oops"
)

type MutationRunnerOptions struct {
	Verbose  bool
	Commit   bool
	Override bool
}

func (o *MutationRunnerOptions) Merge(others ...*MutationRunnerOptions) {
	for _, other := range others {
		o.Verbose = o.Verbose || other.Verbose
		o.Commit = o.Commit || other.Commit
		o.Override = o.Override || other.Override
	}
}

func RunMutations(runner Executor, local *MutationSet, opts ...*MutationRunnerOptions) error {

	var options = MutationRunnerOptions{}
	options.Merge(opts...)

	var err error
	has_changes := true

	if !options.Override {

		var namespace = local.Namespace
		var distant *MutationSet

		if distant, err = runner.GetDBMutationsFromDb(namespace); err != nil {
			return err
		}

		if distant.HasOverrides && distant.Revision < local.Revision {
			runner.Logger().Println("using new_* mutations from previous revision")
			distant = distant.AsNewMutationSet()
		}

		sql_down, sql_up := local.GetMutationsDelta(distant, ITER_SQL)
		meta_down, meta_up := local.GetMutationsDelta(distant, ITER_META)
		has_changes = sql_up.Size() != 0 || meta_up.Size() != 0 || sql_down.Size() != 0 || meta_down.Size() != 0

		if !has_changes {
			// No changes, no tests !
			runner.Logger().Println(au.BrightGreen("≡"), "no changes to apply for namespace", au.BrightMagenta(local.Namespace).String(), "revision", au.BrightGreen(local.Revision).String())
		} else {
			runner.Logger().Println(au.BrightGreen("→"), "applying mutations for namespace", au.BrightMagenta(local.Namespace).String(), "revision", au.BrightGreen(local.Revision).String())

			if sql_down.Size() > 0 {
				var fake_empty_local_set *MutationSet = nil
				_, meta_up = local.GetMutationsDelta(nil, ITER_META)
				meta_down, _ = fake_empty_local_set.GetMutationsDelta(distant, ITER_META)
			}

			if err := runner.Begin(); err != nil {
				return err
			}

			// 1. Start by downing the meta
			if err := meta_down.Run(runner); err != nil {
				return err
			}

			if err := sql_down.Run(runner); err != nil {
				return err
			}

			if err := sql_up.Run(runner); err != nil {
				return err
			}

			if err := meta_up.Run(runner); err != nil {
				return err
			}
		}

	}

	if err := runner.SaveMutations(local); err != nil {
		return err
	}

	if has_changes {
		runner.Logger().Println(au.BrightGreen("🧪"), "performing tests")
		if err := TestMutationSet(runner, local); err != nil {
			return err
		}
	}

	if local.HasOverrides {
		local2 := local.AsNewMutationSet()
		runner.Logger().Println(au.BrightGreen("🧪"), "performing tests with new_*")
		if err := TestMutationSet(runner, local2); err != nil {
			return err
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
			runner.Logger().Println(au.BrightGreen("→"), "no database mutations,applying highest local revision for namespace", namespace)
			if revision, ok := revisions.Revisions[revisions.MaxRevision]; ok {
				if err := RunMutations(runner, revision, opts...); err != nil {
					return err
				}
			}
			continue
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

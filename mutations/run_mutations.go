package mutations

import (
	au "github.com/logrusorgru/aurora"
	"github.com/samber/oops"
)

type MutationRunnerOptions struct {
	TestBefore bool
	Commit     bool
	Override   bool
}

func (o *MutationRunnerOptions) Merge(others ...*MutationRunnerOptions) {
	for _, other := range others {
		o.TestBefore = o.TestBefore || other.TestBefore
		o.Commit = o.Commit || other.Commit
		o.Override = o.Override || other.Override
	}
}

func RunMutations(runner Executor, local *MutationSet, opts ...*MutationRunnerOptions) error {

	var options = MutationRunnerOptions{}
	options.Merge(opts...)

	var err error

	runner.Logger().Println(au.BrightGreen("→"), "applying mutations for namespace", local.Namespace, "revision", local.Revision)

	var namespace = local.Namespace
	var distant *MutationSet

	if distant, err = runner.GetDBMutationsFromDb(namespace); err != nil {
		return err
	}

	sql_down, sql_up := local.GetMutationsDelta(distant, ITER_SQL)
	meta_down, meta_up := local.GetMutationsDelta(distant, ITER_META)

	if sql_up.Size() == 0 && meta_up.Size() == 0 {
		// No changes, no tests !
		runner.Logger().Println(au.BrightGreen("≡"), "no changes to apply")
		return nil
	}

	if sql_down.Size() > 0 {
		_, full_meta_up := local.GetMutationsDelta(nil, ITER_META)
		full_meta_down, _ := distant.GetMutationsDelta(nil, ITER_META)
		meta_down = full_meta_down
		meta_up = full_meta_up
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

	if err := runner.SaveMutations(local); err != nil {
		return err
	}

	runner.Logger().Println(au.BrightGreen("🧪"), "performing tests")
	if err := TestMutationSet(runner, local); err != nil {
		return err
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
			runner.Logger().Println(au.BrightGreen("→"), "applying highest local revision for namespace", namespace)
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
		runner.Logger().Println(au.BrightGreen("💾"), "committing")
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

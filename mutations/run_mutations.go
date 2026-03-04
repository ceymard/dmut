package mutations

import au "github.com/logrusorgru/aurora"

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
	var namespace = local.Namespace
	var distant *MutationSet

	if distant, err = runner.GetDBMutationsFromDb(namespace); err != nil {
		return err
	}

	sql_down, sql_up := local.GetMutationsDelta(distant, ITER_SQL)
	meta_down, meta_up := local.GetMutationsDelta(distant, ITER_META)

	var new_roles = local.Roles.Difference(distant.Roles)
	var removed_roles = distant.Roles.Difference(local.Roles)

	if sql_down.Size() > 0 || removed_roles.Size() > 0 {
		_, full_meta_up := local.GetMutationsDelta(nil, ITER_META)
		full_meta_down, _ := distant.GetMutationsDelta(nil, ITER_META)
		meta_down = full_meta_down
		meta_up = full_meta_up
	}

	// Unfortunately, we have to create missing roles outside of the BEGIN transaction, because the test runner will be in another database and thus cannot access the new roles as transactions cannot span databases.
	for _, role := range new_roles.Values() {
		runner.Logger().Println(au.BrightGreen(""), "adding", au.Bold(au.BrightGreen(role)).String())
		if err := runner.AddRole(namespace, role); err != nil {
			return err
		}
	}

	if err := runner.Begin(); err != nil {
		return err
	}

	// 1. Start by downing the meta
	if err := meta_down.Run(runner); err != nil {
		return err
	}

	// Now, we're inside a transaction, so we can remove the roles without fear of having to undo stuff manually
	if removed_roles.Size() > 0 {
		for _, role := range removed_roles.Values() {
			if err := runner.RemoveRole(namespace, role); err != nil {
				return err
			}
		}
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

	return nil
}

func RunAllMutations(runner Executor, namespaces *MutationNamespace, opts ...*MutationRunnerOptions) (err error) {

	var options = MutationRunnerOptions{}
	options.Merge(opts...)

	if err := runner.Begin(); err != nil {
		return err
	}

	for _, namespace := range namespaces.Values() {
		for _, revision := range namespace.Revisions {
			if err := RunMutations(runner, revision, opts...); err != nil {
				return err
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

// // Apply the mutations on the disk to the database.
// func ApplyMutations(runner Executor, local *MutationSet) error {

// 	namespace := local.Namespace
// 	// Get the currently defined mutations so that we send them to the test runner
// 	current, err := runner.GetDBMutationsFromDb(namespace)
// 	if err != nil {
// 		return err
// 	}

// 	to_up_meta, to_down_meta := local.GetMutationsDelta(current, ITER_META)
// 	to_up_sql, to_down_sql := local.GetMutationsDelta(current, ITER_SQL)

// 	for _, down_runnable := range to_down_meta.Values() {
// 		if err := runner.Run(down_runnable); err != nil {
// 			return err
// 		}
// 	}

// 	for _, down_runnable := range to_down_sql.Values() {
// 		if err := runner.Run(down_runnable); err != nil {
// 			return err
// 		}
// 	}

// 	for _, up_runnable := range to_up_sql.Values() {
// 		if err := runner.Run(up_runnable); err != nil {
// 			return err
// 		}
// 	}

// 	for _, up_runnable := range to_up_meta.Values() {
// 		if err := runner.Run(up_runnable); err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

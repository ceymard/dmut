package mutations

import (
	"context"

	"github.com/ceymard/dmut/v2/mutations/model"
	"github.com/samber/oops"
)

// testPhase checks that a set of mutations, all up, can be taken down and brought back up. It
// runs behind savepoints and leaves the database as it found it. The records it produces are
// held back and shown only if a step fails.
//
// First each mutation is reapplied with all its dependents, as the mode force would do. Then everything goes down and up in one go, with each seed run twice in a row.
func (a *applier) testPhase(ctx context.Context, set *model.Revision) error {
	state, _, err := a.r.LoadState(ctx, set.Namespace)
	if err != nil {
		return err
	}
	// Only the mutations of the set are tested: the others stay as they are.
	for name := range state {
		if _, ok := set.Mutation(name); !ok {
			delete(state, name)
		}
	}
	order, err := set.UpOrder()
	if err != nil {
		return err
	}
	a.r.logger.Info("performing tests", IconKey, IconTestsStarted, "namespace", set.Namespace, "revision", set.Number)
	a.r.BeginCapture(Capture{Phase: "test"})
	err = a.testSteps(ctx, set, state, order)
	a.r.EndCapture(err != nil, "test steps before the failure:")
	if err != nil {
		return err
	}
	a.r.logger.Info("tests passed", IconKey, IconTestsPassed, "namespace", set.Namespace, "revision", set.Number)
	return nil
}

func (a *applier) testSteps(ctx context.Context, set *model.Revision, state model.State, order []*model.Mutation) error {
	all := make([]string, len(order))
	for i, m := range order {
		all[i] = m.Name
		if err := a.testStep(ctx, set, state, []string{m.Name}, false); err != nil {
			return oops.In("test").With("namespace", set.Namespace).With("mutation", m.Name).
				Wrapf(err, "the test phase failed while reapplying %s and its dependents", m.Name)
		}
	}
	if err := a.testStep(ctx, set, state, all, true); err != nil {
		return oops.In("test").With("namespace", set.Namespace).
			Wrapf(err, "the test phase failed while taking every mutation down and up")
	}
	return nil
}

// testStep reapplies names with all their dependents, then rolls back.
func (a *applier) testStep(ctx context.Context, set *model.Revision, state model.State, names []string, seedsTwice bool) error {
	const savepoint = "dmut_test"
	plan, err := model.BuildPlan(model.PlanInput{Recorded: state, Revision: set, Mode: model.ModeNormal, ForceFull: names, Settings: a.values.digests(set.Namespace)})
	if err != nil {
		return err
	}
	if err := a.r.SavePoint(ctx, savepoint); err != nil {
		return err
	}
	a.seedsTwice = seedsTwice
	err = a.execute(ctx, set, state, plan)
	a.seedsTwice = false
	if err != nil {
		return err
	}
	if err := a.r.RollbackToSavepoint(ctx, savepoint); err != nil {
		return err
	}
	return a.r.ReleaseSavepoint(ctx, savepoint)
}

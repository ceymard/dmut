package mutations

import (
	"context"
	"log/slog"
	"sort"

	"github.com/ceymard/dmut/v2/mutations/model"
	"github.com/samber/oops"
)

// Options configure Apply.
type Options struct {
	// Mode is how a run reacts to a change that is not an append; the mode of a revision
	// document wins. The zero value is model.ModeNormal.
	Mode model.Mode
	// Commit keeps the changes; without it the run is rolled back at the end.
	Commit bool
	// Override records the mutations in the database without running them.
	Override bool
	// All applies every revision from the first, even on an empty database.
	All bool
	// Values are the values of the settings named dmut.values.N, by N.
	Values map[string]string
	// Settings, when not nil, gives the value of any setting that is neither dmut.env.X nor
	// dmut.values.N. It receives the name split on dots and returns nil for unset.
	Settings func(path []string) *string
	// Logger receives dmut's progress records and notices (and SQL at debug level);
	// slog.Default() when nil.
	Logger *slog.Logger
}

// Apply loads the mutation files under paths, with dmut's own, and applies them to the
// database at uri in one transaction.
func Apply(ctx context.Context, uri string, paths []string, opts Options) error {
	project, err := LoadProject(paths...)
	if err != nil {
		return err
	}
	r, err := NewRunner(ctx, uri, opts.Logger)
	if err != nil {
		return err
	}
	defer r.Close(ctx)
	return r.ApplyProject(ctx, project, opts)
}

// applier is a run in progress.
type applier struct {
	r      *Runner
	opts   Options
	values settingValues
}

// ApplyProject applies the namespaces of project, in order, in one transaction. A namespace
// starts from the revision the database is at, or from its highest one on an empty database.
// The transaction is rolled back on error, and unless opts.Commit is set.
func (r *Runner) ApplyProject(ctx context.Context, project *model.Project, opts Options) (err error) {
	for _, w := range project.Warnings {
		r.logger.Warn(w.Message, "file", w.File, "mutation", w.Mutation, "statement", w.Position)
	}
	a := &applier{r: r, opts: opts, values: resolveSettings(project, opts)}

	if err := r.Begin(ctx); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			r.logger.Info("rolling back", IconKey, IconRolledBack)
			cleanup, cancel := CleanupContext(ctx)
			defer cancel()
			_ = r.Rollback(cleanup)
		}
	}()

	for _, ns := range project.Namespaces {
		if err := a.applyNamespace(ctx, project, ns); err != nil {
			return err
		}
	}

	r.logger.Info("no errors", IconKey, IconNoErrors)
	if opts.Commit {
		return r.Commit(ctx)
	}
	r.logger.Info("rolling back, nothing to commit")
	return r.Rollback(ctx)
}

func (a *applier) applyNamespace(ctx context.Context, project *model.Project, ns *model.Namespace) error {
	_, current, err := a.r.LoadState(ctx, ns.Name)
	if err != nil {
		return err
	}
	start := max(current, ns.Min)
	if current == 0 {
		start = ns.Max
		if a.opts.All {
			start = ns.Min
		} else {
			a.r.logger.Info("no database mutations, applying highest local revision", "namespace", ns.Name, "revision", ns.Max)
		}
	}
	for n := start; n <= ns.Max; n++ {
		if rev, ok := ns.Revisions[n]; ok {
			if err := a.applyRevision(ctx, project, rev); err != nil {
				return err
			}
		}
	}
	return nil
}

// applyRevision carries the recorded state of a namespace to one revision.
func (a *applier) applyRevision(ctx context.Context, project *model.Project, rev *model.Revision) error {
	state, current, err := a.r.LoadState(ctx, rev.Namespace)
	if err != nil {
		return err
	}
	mode := a.opts.Mode
	if rev.Mode != "" {
		mode = rev.Mode
	}
	input := model.PlanInput{Recorded: state, Revision: rev, Mode: mode, Settings: a.values.digests(rev.Namespace)}
	plan, err := model.BuildPlan(input)
	if err != nil {
		return err
	}

	// Every name a statement may read exists from now on, empty.
	names := usedSettingNames(project, state)
	if err := a.r.clearSettings(ctx, location{}, names); err != nil {
		return err
	}

	if plan.Empty() {
		a.r.logger.Info("no changes to apply", "namespace", rev.Namespace, "revision", rev.Number)
	} else {
		a.r.logger.Info("applying mutations", "namespace", rev.Namespace, "revision", rev.Number)
		if plan, err = a.run(ctx, rev, state, input, plan); err != nil {
			return err
		}
		if err := a.writeRecords(ctx, rev, state, plan); err != nil {
			return err
		}
	}
	if current != rev.Number || !plan.Empty() {
		if err := a.r.SetRevision(ctx, rev.Namespace, rev.Number); err != nil {
			return err
		}
	}
	return a.recordOverrides(ctx, rev)
}

// run executes the plan. In the mode force, a failing plan is rolled back and tried again with
// the mutations that needed a down, and their dependents, reapplied in full. It returns the plan
// that ran.
func (a *applier) run(ctx context.Context, rev *model.Revision, state model.State, input model.PlanInput, plan *model.Plan) (*model.Plan, error) {
	if a.opts.Override && rev.Namespace != DmutNamespace {
		return plan, nil
	}
	if input.Mode != model.ModeForce {
		return plan, a.execute(ctx, rev, state, plan)
	}

	const savepoint = "dmut_attempt"
	if err := a.r.SavePoint(ctx, savepoint); err != nil {
		return nil, err
	}
	err := a.execute(ctx, rev, state, plan)
	if err == nil {
		return plan, a.r.ReleaseSavepoint(ctx, savepoint)
	}
	var forced []string
	for _, step := range plan.Downs {
		if step.Kind == model.StepTail || step.Kind == model.StepReapply {
			forced = append(forced, step.Mutation)
		}
	}
	if len(forced) == 0 {
		return nil, err
	}
	a.r.logger.Info("the change failed, reapplying the mutations that went down and their dependents", append([]any{"namespace", rev.Namespace, "revision", rev.Number}, ErrorAttrs(err)...)...)
	if err := a.r.RollbackToSavepoint(ctx, savepoint); err != nil {
		return nil, err
	}
	input.ForceFull = forced
	if plan, err = model.BuildPlan(input); err != nil {
		return nil, err
	}
	if err := a.execute(ctx, rev, state, plan); err != nil {
		return nil, err
	}
	return plan, a.r.ReleaseSavepoint(ctx, savepoint)
}

// execute runs the downs, then the ups, of a plan.
func (a *applier) execute(ctx context.Context, rev *model.Revision, state model.State, plan *model.Plan) error {
	for _, step := range plan.Downs {
		a.logStep(rev, step, "down", IconDown)
		if err := a.down(ctx, rev.Namespace, state[step.Mutation], step.From); err != nil {
			return err
		}
	}
	for _, step := range plan.Ups {
		a.logStep(rev, step, "up", IconUp)
		m, _ := rev.Mutation(step.Mutation)
		if err := a.up(ctx, rev.Namespace, m, step.From); err != nil {
			return err
		}
	}
	return nil
}

func (a *applier) logStep(rev *model.Revision, step model.Step, dir, icon string) {
	attrs := []any{IconKey, icon, "namespace", rev.Namespace, "revision", rev.Number, "mutation", step.Mutation, "dir", dir}
	if step.Reason != "" {
		attrs = append(attrs, "reason", step.Reason, "cause", step.Cause)
	}
	a.r.logger.Info("run", attrs...)
}

// recordOf is what is stored for m: its statements with their digests and saved settings.
func (a *applier) recordOf(rev *model.Revision, m *model.Mutation) *model.RecordedMutation {
	rec := model.Record(m, rev.Number, a.values.digests(rev.Namespace))
	for i := range rec.Statements {
		rec.Statements[i].Saved = a.values.saved(m, i)
	}
	return rec
}

// writeRecords makes the stored state match the plan that ran: it forgets the statements that
// went down, records the new mutations and the changed rows, and records the statements that went up.
func (a *applier) writeRecords(ctx context.Context, rev *model.Revision, state model.State, plan *model.Plan) error {
	var removed []string
	for _, step := range plan.Downs {
		switch step.Kind {
		case model.StepRemove:
			removed = append(removed, step.Mutation)
		default:
			if err := a.r.DeleteStatementsFrom(ctx, rev.Namespace, step.Mutation, step.From); err != nil {
				return err
			}
		}
	}
	if err := a.r.DeleteMutations(ctx, rev.Namespace, removed); err != nil {
		return err
	}

	saved := map[string]bool{}
	for _, name := range plan.Save {
		saved[name] = true
	}
	ups := map[string]model.Step{}
	for _, step := range plan.Ups {
		ups[step.Mutation] = step
		saved[step.Mutation] = saved[step.Mutation] || state[step.Mutation] == nil
	}
	names := make([]string, 0, len(saved)+len(ups))
	for name := range saved {
		names = append(names, name)
	}
	for name := range ups {
		if !saved[name] {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		m, _ := rev.Mutation(name)
		rec := a.recordOf(rev, m)
		if saved[name] {
			if err := a.r.SaveMutation(ctx, rev.Namespace, rec); err != nil {
				return err
			}
		}
		if step, ok := ups[name]; ok {
			if err := a.r.InsertStatements(ctx, rev.Namespace, name, step.From, rec.Statements[step.From:]); err != nil {
				return err
			}
		}
	}
	return nil
}

// recordOverrides applies the bookkeeping of a revision: the mutations in delete: are forgotten
// without running anything, and those in override: are recorded in place of what ran.
func (a *applier) recordOverrides(ctx context.Context, rev *model.Revision) error {
	if err := a.r.DeleteMutations(ctx, rev.Namespace, rev.Delete); err != nil {
		return err
	}
	deleted := map[string]bool{}
	for _, name := range rev.Delete {
		deleted[name] = true
	}
	for _, m := range rev.Override {
		if deleted[m.Name] {
			continue
		}
		rec := a.recordOf(rev, m)
		if err := a.r.DeleteStatementsFrom(ctx, rev.Namespace, m.Name, 0); err != nil {
			return err
		}
		if err := a.r.SaveMutation(ctx, rev.Namespace, rec); err != nil {
			return oops.In("apply").With("mutation", m.Name).Wrap(err)
		}
		if err := a.r.InsertStatements(ctx, rev.Namespace, m.Name, 0, rec.Statements); err != nil {
			return err
		}
	}
	return nil
}

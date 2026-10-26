package model

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/samber/oops"
)

// RecordedStatement is a statement as stored in the database.
type RecordedStatement struct {
	Kind Kind
	Up   string
	Down string
	// Digest is the text produced by Digest.
	Digest string
	// Saved holds the values of the saved settings, by name; a nil value was unset.
	Saved map[string]*string
}

// RecordedDep is a dependency entry as stored: its pattern, triggers and the names it matched.
type RecordedDep struct {
	Entry     string
	ReapplyOn []Trigger
	Matched   []string
}

// RecordedMutation is a mutation as stored in the database.
type RecordedMutation struct {
	Name       string
	File       string
	Revision   int
	Structural bool
	ReapplyOn  []Trigger
	Statements []RecordedStatement
	Deps       []RecordedDep
}

// State is the recorded mutations of one namespace, by name.
type State map[string]*RecordedMutation

// SettingsFunc returns the tracked settings digests of statement i (0-based) of m.
// A nil SettingsFunc means no statement has tracked settings.
type SettingsFunc func(m *Mutation, i int) []SettingDigest

func (f SettingsFunc) digest(m *Mutation, i int) string {
	var settings []SettingDigest
	if f != nil {
		settings = f(m, i)
	}
	return Digest(m.Statements[i].DataHash, settings)
}

// Record builds what dmut stores for m at the given revision.
func Record(m *Mutation, revision int, settings SettingsFunc) *RecordedMutation {
	rec := &RecordedMutation{
		Name: m.Name, File: m.File, Revision: revision, Structural: m.Structural, ReapplyOn: m.ReapplyOn,
	}
	for i, st := range m.Statements {
		rec.Statements = append(rec.Statements, RecordedStatement{Kind: st.Kind, Up: st.Up, Down: st.Down, Digest: settings.digest(m, i)})
	}
	for i, entry := range m.Deps {
		dep := RecordedDep{Entry: entry.Pattern, ReapplyOn: entry.ReapplyOn}
		for _, d := range m.Matched[i] {
			dep.Matched = append(dep.Matched, d.Name)
		}
		rec.Deps = append(rec.Deps, dep)
	}
	return rec
}

// StepKind says why a step runs.
type StepKind string

const (
	// StepNew brings up a mutation that is not recorded.
	StepNew StepKind = "new"
	// StepAppend brings up statements added at the end of a mutation.
	StepAppend StepKind = "append"
	// StepTail takes down and brings up the statements from a changed one to the end.
	StepTail StepKind = "tail"
	// StepReapply takes a whole mutation down and brings it back up.
	StepReapply StepKind = "reapply"
	// StepRemove takes down a mutation that no longer exists.
	StepRemove StepKind = "remove"
)

// Step is one mutation going down or up from statement From (0-based).
type Step struct {
	Mutation string
	Kind     StepKind
	From     int
	// Reason is the name of the trigger or "setting" that caused the step; empty for a plain edit.
	Reason string
	// Cause names the dependency or the setting behind Reason.
	Cause string
}

// Plan is what a run does, in order: all Downs, then all Ups. Downs go from dependents to
// dependencies, Ups the other way.
type Plan struct {
	Downs []Step
	Ups   []Step
	// Save lists the mutations whose row and dependencies must be written, even with no step.
	Save []string
}

// Empty reports whether the plan does nothing.
func (p *Plan) Empty() bool { return len(p.Downs) == 0 && len(p.Ups) == 0 && len(p.Save) == 0 }

// PlanInput is everything BuildPlan needs.
type PlanInput struct {
	Recorded State
	Revision *Revision
	Mode     Mode
	// ForceFull names mutations to reapply in full with all their dependents,
	// whatever the triggers say.
	ForceFull []string
	Settings  SettingsFunc
}

// change is what differs between a recorded mutation and its new definition.
type change struct {
	m   *Mutation
	rec *RecordedMutation
	// k is the number of leading statements that are unchanged.
	k             int
	reason, cause string
}

func (c *change) edited() bool   { return c.rec != nil && c.k < len(c.rec.Statements) }
func (c *change) appended() bool { return c.rec != nil && !c.edited() && len(c.m.Statements) > c.k }

// BuildPlan works out what has to go down and up to carry the recorded state to the new
// revision. It does not touch a database. In the mode safe it refuses anything but appends.
func BuildPlan(in PlanInput) (*Plan, error) {
	rev := in.Revision
	order, err := rev.UpOrder()
	if err != nil {
		return nil, err
	}
	changes := map[string]*change{}
	for _, m := range rev.Mutations {
		c := &change{m: m, rec: in.Recorded[m.Name]}
		if c.rec != nil {
			c.compare(in.Settings)
		}
		changes[m.Name] = c
	}
	removed := map[string]bool{}
	for name := range in.Recorded {
		if _, ok := rev.byName[name]; !ok {
			removed[name] = true
		}
	}

	reapplied, appended := map[string]*Step{}, map[string]bool{}
	forceFull(in, reapplied)
	for _, m := range order {
		c := changes[m.Name]
		if c.rec == nil {
			continue
		}
		if _, forced := reapplied[m.Name]; !forced {
			if reason, cause := triggered(c, changes, reapplied, appended, removed); reason != "" {
				reapplied[m.Name] = &Step{Mutation: m.Name, Kind: StepReapply, Reason: string(reason), Cause: cause}
			}
		}
		if _, ok := reapplied[m.Name]; !ok && c.appended() {
			appended[m.Name] = true
		}
	}

	nodes, err := globalOrder(in, order, removed)
	if err != nil {
		return nil, err
	}
	plan := &Plan{}
	for _, name := range nodes {
		if removed[name] {
			plan.Downs = append(plan.Downs, Step{Mutation: name, Kind: StepRemove})
			continue
		}
		c := changes[name]
		switch step, isReapply := reapplied[name]; {
		case isReapply:
			plan.Downs = append(plan.Downs, *step)
			plan.Ups = append(plan.Ups, Step{Mutation: name, Kind: StepReapply, Reason: step.Reason, Cause: step.Cause})
		case c.rec == nil:
			plan.Ups = append(plan.Ups, Step{Mutation: name, Kind: StepNew})
		case c.edited():
			plan.Downs = append(plan.Downs, Step{Mutation: name, Kind: StepTail, From: c.k, Reason: c.reason, Cause: c.cause})
			plan.Ups = append(plan.Ups, Step{Mutation: name, Kind: StepTail, From: c.k, Reason: c.reason, Cause: c.cause})
		case c.appended():
			plan.Ups = append(plan.Ups, Step{Mutation: name, Kind: StepAppend, From: c.k})
		}
		if c.rec == nil || metaChanged(c, in.Settings) {
			plan.Save = append(plan.Save, name)
		}
	}
	slices.Reverse(plan.Downs)

	if in.Mode == ModeSafe {
		if err := refuseNonAppends(plan, changes); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

// compare finds how many leading statements are unchanged, and why the first one differs.
func (c *change) compare(settings SettingsFunc) {
	rec, m := c.rec, c.m
	for c.k < len(rec.Statements) && c.k < len(m.Statements) {
		r, s := rec.Statements[c.k], m.Statements[c.k]
		digest := settings.digest(m, c.k)
		text, _ := TextHash(r.Kind, r.Up, r.Down)
		if r.Kind == s.Kind && text == s.TextHash {
			if r.Digest == digest {
				c.k++
				continue
			}
			// Same text: the data or a setting changed. A setting is named when it is the one that did.
			rData, rSettings := ParseDigest(r.Digest)
			if rData == s.DataHash {
				c.reason = "setting"
				c.cause = changedSetting(rSettings, SettingsDigests(settings, m, c.k))
			}
		}
		return
	}
}

// SettingsDigests returns the tracked settings digests of statement i, or none.
func SettingsDigests(f SettingsFunc, m *Mutation, i int) []SettingDigest {
	if f == nil {
		return nil
	}
	return f(m, i)
}

func changedSetting(old, now []SettingDigest) string {
	for i := range max(len(old), len(now)) {
		switch {
		case i >= len(old):
			return now[i].Name
		case i >= len(now):
			return old[i].Name
		case old[i] != now[i]:
			if old[i].Name != now[i].Name {
				return now[i].Name
			}
			return old[i].Name
		}
	}
	return ""
}

// forceFull marks the forced mutations and everything that depends on them as reapplied.
func forceFull(in PlanInput, reapplied map[string]*Step) {
	dependents := map[string][]string{}
	for _, m := range in.Revision.Mutations {
		for _, d := range m.Dependencies() {
			dependents[d.Name] = append(dependents[d.Name], m.Name)
		}
	}
	var mark func(name, cause string)
	mark = func(name, cause string) {
		if _, done := reapplied[name]; done || in.Recorded[name] == nil {
			return
		}
		step := &Step{Mutation: name, Kind: StepReapply}
		if cause != "" {
			step.Reason, step.Cause = string(DepReapplied), cause
		}
		reapplied[name] = step
		for _, dep := range dependents[name] {
			mark(dep, name)
		}
	}
	for _, name := range in.ForceFull {
		if _, ok := in.Revision.byName[name]; ok {
			mark(name, "")
		}
	}
}

// triggered returns the first trigger that makes c reapply, with the name that caused it.
func triggered(c *change, changes map[string]*change, reapplied map[string]*Step, appended, removed map[string]bool) (Trigger, string) {
	m, rec := c.m, c.rec

	// Triggers per dependency, as the union over the entries that match it.
	sets := map[string][]Trigger{}
	for _, p := range m.Parents {
		sets[p.Name] = append(sets[p.Name], m.DefaultReapplyOn()...)
	}
	for i, entry := range m.Deps {
		for _, d := range m.Matched[i] {
			sets[d.Name] = append(sets[d.Name], m.EffectiveReapplyOn(entry)...)
		}
	}
	// A removed dependency counts through the entries (or ancestors) that matched it.
	for _, dep := range rec.Deps {
		set := m.DefaultReapplyOn()
		for _, entry := range m.Deps {
			if entry.Pattern == dep.Entry {
				set = m.EffectiveReapplyOn(entry)
			}
		}
		for _, name := range dep.Matched {
			if removed[name] {
				sets[name] = append(sets[name], set...)
			}
		}
	}
	for name := range removed {
		if strings.HasPrefix(rec.Name, name+".") {
			sets[name] = append(sets[name], m.DefaultReapplyOn()...)
		}
	}

	names := make([]string, 0, len(sets))
	for name := range sets {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if (reapplied[name] != nil || removed[name]) && slices.Contains(sets[name], DepReapplied) {
			return DepReapplied, name
		}
	}
	for _, name := range names {
		if appended[name] && slices.Contains(sets[name], DepAppended) {
			return DepAppended, name
		}
	}

	// Growth and shrinking compare the stored matched names with the new ones, entry by entry.
	stored := map[string][]string{}
	for _, dep := range rec.Deps {
		stored[dep.Entry] = append(stored[dep.Entry], dep.Matched...)
	}
	current := map[string]bool{}
	for i, entry := range m.Deps {
		current[entry.Pattern] = true
		set := m.EffectiveReapplyOn(entry)
		var now []string
		for _, d := range m.Matched[i] {
			now = append(now, d.Name)
		}
		if name := firstMissing(now, stored[entry.Pattern]); name != "" && slices.Contains(set, DepsGrow) {
			return DepsGrow, name
		}
		if name := firstMissing(stored[entry.Pattern], now); name != "" && slices.Contains(set, DepsShrink) {
			return DepsShrink, name
		}
	}
	for _, dep := range rec.Deps {
		if !current[dep.Entry] && slices.Contains(m.DefaultReapplyOn(), DepsShrink) {
			if name := firstMissing(dep.Matched, nil); name != "" {
				return DepsShrink, name
			}
		}
	}
	return "", ""
}

// firstMissing returns the first of a (sorted by name) that b does not hold.
func firstMissing(a, b []string) string {
	for _, name := range a {
		if !slices.Contains(b, name) {
			return name
		}
	}
	return ""
}

// metaChanged reports whether the mutation row or its dependencies differ from what is stored.
func metaChanged(c *change, settings SettingsFunc) bool {
	now := Record(c.m, c.rec.Revision, settings)
	rec := c.rec
	if now.File != rec.File || now.Structural != rec.Structural || !sameTriggers(now.ReapplyOn, rec.ReapplyOn) || len(now.Deps) != len(rec.Deps) {
		return true
	}
	for i, d := range now.Deps {
		r := rec.Deps[i]
		if d.Entry != r.Entry || !sameTriggers(d.ReapplyOn, r.ReapplyOn) || !slices.Equal(d.Matched, r.Matched) {
			return true
		}
	}
	return false
}

func sameTriggers(a, b []Trigger) bool {
	return (a == nil) == (b == nil) && slices.Equal(a, b)
}

// globalOrder orders live and removed mutations with dependencies first. Live mutations use the
// new graph; a removed mutation uses the dependencies recorded for it, and a live one that was
// recorded as depending on it goes down before it.
func globalOrder(in PlanInput, live []*Mutation, removed map[string]bool) ([]string, error) {
	deps := map[string][]string{}
	nodes := []string{}
	for _, m := range live {
		nodes = append(nodes, m.Name)
		for _, d := range m.Dependencies() {
			deps[m.Name] = append(deps[m.Name], d.Name)
		}
		if rec := in.Recorded[m.Name]; rec != nil {
			for _, name := range recordedDependencies(rec, in.Recorded) {
				if removed[name] {
					deps[m.Name] = append(deps[m.Name], name)
				}
			}
		}
	}
	for name := range removed {
		nodes = append(nodes, name)
		deps[name] = recordedDependencies(in.Recorded[name], in.Recorded)
	}
	sort.Strings(nodes)

	const visiting, done = 1, 2
	state := map[string]int{}
	var out, path []string
	var visit func(name string) error
	visit = func(name string) error {
		switch state[name] {
		case done:
			return nil
		case visiting:
			cycle := append(slices.Clone(path[slices.Index(path, name):]), name)
			return oops.In("model").Errorf("the order of downs has a cycle between the stored and the new dependencies: %s", strings.Join(cycle, " -> "))
		}
		state[name] = visiting
		path = append(path, name)
		sorted := slices.Clone(deps[name])
		sort.Strings(sorted)
		for _, d := range sorted {
			if err := visit(d); err != nil {
				return err
			}
		}
		path = path[:len(path)-1]
		state[name] = done
		out = append(out, name)
		return nil
	}
	for _, name := range nodes {
		if err := visit(name); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// recordedDependencies returns the names a stored mutation matched, plus its stored ancestors.
func recordedDependencies(rec *RecordedMutation, all State) []string {
	seen := map[string]bool{}
	var out []string
	add := func(name string) {
		if name != rec.Name && !seen[name] && all[name] != nil {
			seen[name] = true
			out = append(out, name)
		}
	}
	for _, dep := range rec.Deps {
		for _, name := range dep.Matched {
			add(name)
		}
	}
	for i := len(rec.Name) - 1; i > 0; i-- {
		if rec.Name[i] == '.' {
			add(rec.Name[:i])
		}
	}
	return out
}

// refuseNonAppends is the check of the mode safe: only new mutations and appends may run.
func refuseNonAppends(plan *Plan, changes map[string]*change) error {
	var refused []string
	for _, step := range plan.Downs {
		c := changes[step.Mutation]
		switch {
		case step.Kind == StepRemove:
			refused = append(refused, fmt.Sprintf("mutation %s is removed", step.Mutation))
		case step.Kind == StepTail:
			refused = append(refused, fmt.Sprintf("mutation %s changes at statement %d", step.Mutation, step.From+1))
		case step.Kind == StepReapply && c.m.Structural:
			refused = append(refused, fmt.Sprintf("structural mutation %s would be reapplied", step.Mutation))
		}
	}
	for name, c := range changes {
		if c.rec != nil && c.rec.Structural != c.m.Structural {
			refused = append(refused, fmt.Sprintf("mutation %s changes its structural flag", name))
		}
	}
	if len(refused) == 0 {
		return nil
	}
	sort.Strings(refused)
	return oops.In("model").Errorf("the mode safe only accepts appends: %s", strings.Join(refused, " ; "))
}

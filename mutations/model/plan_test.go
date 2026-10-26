package model

import (
	"fmt"
	"strings"
	"testing"
)

type planOpts struct {
	mode        Mode
	force       []string
	oldSettings SettingsFunc
	newSettings SettingsFunc
}

// planBetween records the mutations of old as applied, then plans the move to new.
func planBetween(t *testing.T, old, new map[string]string, o planOpts) (*Plan, error) {
	t.Helper()
	state := State{}
	for _, m := range mustLoad(t, old).Namespaces[0].Revisions[1].Mutations {
		state[m.Name] = Record(m, 1, o.oldSettings)
	}
	rev := mustLoad(t, new).Namespaces[0].Revisions[1]
	return BuildPlan(PlanInput{Recorded: state, Revision: rev, Mode: o.mode, ForceFull: o.force, Settings: o.newSettings})
}

func mustPlan(t *testing.T, old, new map[string]string, o planOpts) *Plan {
	t.Helper()
	p, err := planBetween(t, old, new, o)
	if err != nil {
		t.Fatalf("plan failed: %v", err)
	}
	return p
}

// summary writes a plan as "down:a(tail 1) up:b(new)" to compare in one line.
func (p *Plan) summary() string {
	var parts []string
	for _, s := range p.Downs {
		parts = append(parts, "down:"+s.describe())
	}
	for _, s := range p.Ups {
		parts = append(parts, "up:"+s.describe())
	}
	return strings.Join(parts, " ")
}

func (s Step) describe() string {
	out := fmt.Sprintf("%s(%s", s.Mutation, s.Kind)
	if s.Kind == StepTail || s.Kind == StepAppend {
		out += fmt.Sprintf(" %d", s.From)
	}
	if s.Reason != "" {
		out += " " + s.Reason
		if s.Cause != "" {
			out += ":" + s.Cause
		}
	}
	return out + ")"
}

func wantPlan(t *testing.T, p *Plan, want string) {
	t.Helper()
	if got := p.summary(); got != want {
		t.Errorf("plan:\n got  %s\n want %s", got, want)
	}
}

func seeds(n int) string {
	var b strings.Builder
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, "  - seed: select %d\n", i)
	}
	return b.String()
}

func files(pairs ...string) map[string]string {
	out := map[string]string{}
	for i := 0; i < len(pairs); i += 2 {
		out[pairs[i]+".yml"] = pairs[i+1]
	}
	return out
}

func TestPlanNothingToDo(t *testing.T) {
	f := files("a", "sql:\n"+seeds(2), "b", "deps: [a]\n")
	p := mustPlan(t, f, f, planOpts{})
	if !p.Empty() {
		t.Errorf("want an empty plan, got %s %v", p.summary(), p.Save)
	}
}

func TestPlanNewAppendAndTail(t *testing.T) {
	old := files("a", "sql:\n"+seeds(3), "b", "sql:\n"+seeds(2))
	// a gets a statement at the end, b changes its second statement, c is new.
	new := files("a", "sql:\n"+seeds(4), "b", "sql:\n  - seed: select 1\n  - seed: select 99\n", "c", "sql:\n"+seeds(1))
	p := mustPlan(t, old, new, planOpts{})
	wantPlan(t, p, "down:b(tail 1) up:a(append 3) up:b(tail 1) up:c(new)")
	if len(p.Save) != 1 || p.Save[0] != "c" {
		t.Errorf("only the new mutation needs saving, got %v", p.Save)
	}
}

func TestPlanStatementsRemovedAtTheEnd(t *testing.T) {
	p := mustPlan(t, files("a", "sql:\n"+seeds(3)), files("a", "sql:\n"+seeds(2)), planOpts{})
	wantPlan(t, p, "down:a(tail 2) up:a(tail 2)")
}

func TestPlanSwapRedoesFromFirstSwappedPosition(t *testing.T) {
	old := files("a", "structural: true\nsql:\n  - create table t (a int)\n  - alter table t add column b int\n  - alter table t add column c int\n")
	new := files("a", "structural: true\nsql:\n  - create table t (a int)\n  - alter table t add column c int\n  - alter table t add column b int\n")
	wantPlan(t, mustPlan(t, old, new, planOpts{}), "down:a(tail 1) up:a(tail 1)")
}

func TestPlanWhitespaceIsNotAChange(t *testing.T) {
	old := files("a", "sql:\n  - seed: select   1\n")
	new := files("a", "sql:\n  - seed: select 1 -- comment\n")
	if p := mustPlan(t, old, new, planOpts{}); !p.Empty() {
		t.Errorf("got %s", p.summary())
	}
}

func TestPlanRemoval(t *testing.T) {
	old := files("a", "sql:\n"+seeds(1), "a.b", "sql:\n"+seeds(1), "c", "sql:\n"+seeds(1))
	new := files("c", "sql:\n"+seeds(1))
	// The child goes down before its parent.
	wantPlan(t, mustPlan(t, old, new, planOpts{}), "down:a.b(remove) down:a(remove)")
}

func TestPlanReapplyOnDependencyRemoved(t *testing.T) {
	// b depended on a ; a goes away and b stops listing it. b reapplies, and goes down first.
	old := files("a", "sql:\n"+seeds(1), "b", "deps: [a]\nsql:\n"+seeds(1))
	new := files("b", "sql:\n"+seeds(1))
	wantPlan(t, mustPlan(t, old, new, planOpts{}),
		"down:b(reapply dep_reapplied:a) down:a(remove) up:b(reapply dep_reapplied:a)")

	// A structural dependent does not react by default.
	old = files("a", "sql:\n"+seeds(1), "b", "structural: true\ndeps: [a]\nsql:\n"+seeds(1))
	new = files("b", "structural: true\nsql:\n"+seeds(1))
	wantPlan(t, mustPlan(t, old, new, planOpts{}), "down:a(remove)")
}

func TestPlanDepAppended(t *testing.T) {
	old := files("a", "structural: true\nsql:\n  - create table t (a int)\n", "b", "deps: [a]\nreapply_on: [dep_appended]\nsql:\n"+seeds(1),
		"c", "deps: [a]\nsql:\n"+seeds(1))
	new := files("a", "structural: true\nsql:\n  - create table t (a int)\n  - alter table t add column b int\n", "b", "deps: [a]\nreapply_on: [dep_appended]\nsql:\n"+seeds(1),
		"c", "deps: [a]\nsql:\n"+seeds(1))
	// c uses the default (dep_reapplied only) and does not move.
	wantPlan(t, mustPlan(t, old, new, planOpts{}), "down:b(reapply dep_appended:a) up:a(append 1) up:b(reapply dep_appended:a)")

	// A tail change of a is not an append: dep_appended does not fire.
	new = files("a", "structural: true\nsql:\n  - create table u (a int)\n", "b", "deps: [a]\nreapply_on: [dep_appended]\nsql:\n"+seeds(1),
		"c", "deps: [a]\nsql:\n"+seeds(1))
	wantPlan(t, mustPlan(t, old, new, planOpts{}), "down:a(tail 0) up:a(tail 0)")
}

func TestPlanDepReappliedCascades(t *testing.T) {
	// z goes away: a (default) reapplies, then b (depends on a), then c (depends on b).
	old := files("z", "sql:\n"+seeds(1), "a", "deps: [z]\nsql:\n"+seeds(1), "b", "deps: [a]\nsql:\n"+seeds(1),
		"c", "deps: [b]\nsql:\n"+seeds(1), "d", "structural: true\ndeps: [b]\nsql:\n"+seeds(1))
	new := files("a", "sql:\n"+seeds(1), "b", "deps: [a]\nsql:\n"+seeds(1), "c", "deps: [b]\nsql:\n"+seeds(1),
		"d", "structural: true\ndeps: [b]\nsql:\n"+seeds(1))
	wantPlan(t, mustPlan(t, old, new, planOpts{}),
		"down:c(reapply dep_reapplied:b) down:b(reapply dep_reapplied:a) down:a(reapply dep_reapplied:z) down:z(remove) "+
			"up:a(reapply dep_reapplied:z) up:b(reapply dep_reapplied:a) up:c(reapply dep_reapplied:b)")
}

func TestPlanDepsGrowAndShrink(t *testing.T) {
	old := files("x.1", "sql:\n"+seeds(1), "x.2", "sql:\n"+seeds(1),
		"g", "deps: ['x.*']\nreapply_on: [deps_grow]\nsql:\n"+seeds(1),
		"s", "deps: ['x.*']\nreapply_on: [deps_shrink]\nsql:\n"+seeds(1),
		"n", "deps: ['x.*']\nreapply_on: []\nsql:\n"+seeds(1))
	// x.3 appears: only g reacts.
	grown := files("x.1", "sql:\n"+seeds(1), "x.2", "sql:\n"+seeds(1), "x.3", "sql:\n"+seeds(1),
		"g", "deps: ['x.*']\nreapply_on: [deps_grow]\nsql:\n"+seeds(1),
		"s", "deps: ['x.*']\nreapply_on: [deps_shrink]\nsql:\n"+seeds(1),
		"n", "deps: ['x.*']\nreapply_on: []\nsql:\n"+seeds(1))
	wantPlan(t, mustPlan(t, old, grown, planOpts{}), "down:g(reapply deps_grow:x.3) up:x.3(new) up:g(reapply deps_grow:x.3)")

	// x.2 goes away: only s reacts (its set holds deps_shrink, not dep_reapplied).
	shrunk := files("x.1", "sql:\n"+seeds(1),
		"g", "deps: ['x.*']\nreapply_on: [deps_grow]\nsql:\n"+seeds(1),
		"s", "deps: ['x.*']\nreapply_on: [deps_shrink]\nsql:\n"+seeds(1),
		"n", "deps: ['x.*']\nreapply_on: []\nsql:\n"+seeds(1))
	wantPlan(t, mustPlan(t, old, shrunk, planOpts{}), "down:s(reapply deps_shrink:x.2) down:x.2(remove) up:s(reapply deps_shrink:x.2)")
}

func TestPlanUnionOfEntriesOnOneDependency(t *testing.T) {
	old := files("a", "structural: true\nsql:\n  - create table t (a int)\n",
		"b", "deps: [{name: a, reapply_on: []}, {name: '*', reapply_on: [dep_appended]}]\nsql:\n"+seeds(1))
	new := files("a", "structural: true\nsql:\n  - create table t (a int)\n  - alter table t add column c int\n",
		"b", "deps: [{name: a, reapply_on: []}, {name: '*', reapply_on: [dep_appended]}]\nsql:\n"+seeds(1))
	wantPlan(t, mustPlan(t, old, new, planOpts{}), "down:b(reapply dep_appended:a) up:a(append 1) up:b(reapply dep_appended:a)")
}

func TestPlanChangedDepsWithoutTriggerDoesNothing(t *testing.T) {
	old := files("a", "sql:\n"+seeds(1), "b", "sql:\n"+seeds(1), "c", "deps: [a]\nreapply_on: [dep_reapplied]\nsql:\n"+seeds(1))
	new := files("a", "sql:\n"+seeds(1), "b", "sql:\n"+seeds(1), "c", "deps: [b]\nreapply_on: [dep_reapplied]\nsql:\n"+seeds(1))
	p := mustPlan(t, old, new, planOpts{})
	if len(p.Downs) != 0 || len(p.Ups) != 0 || len(p.Save) != 1 || p.Save[0] != "c" {
		t.Errorf("the new deps should be saved without any step: %s %v", p.summary(), p.Save)
	}
}

func TestPlanForceReappliesDependents(t *testing.T) {
	f := files("a", "structural: true\nsql:\n  - create table t (a int)\n", "b", "structural: true\ndeps: [a]\nreapply_on: []\nsql:\n"+seeds(1),
		"c", "deps: [b]\nsql:\n"+seeds(1), "d", "sql:\n"+seeds(1))
	p := mustPlan(t, f, f, planOpts{force: []string{"a"}})
	wantPlan(t, p, "down:c(reapply dep_reapplied:b) down:b(reapply dep_reapplied:a) down:a(reapply) "+
		"up:a(reapply) up:b(reapply dep_reapplied:a) up:c(reapply dep_reapplied:b)")
}

func TestPlanStructuralFlagChange(t *testing.T) {
	old := files("a", "sql:\n"+seeds(1))
	new := files("a", "structural: true\nsql:\n"+seeds(1))
	p := mustPlan(t, old, new, planOpts{})
	if len(p.Downs)+len(p.Ups) != 0 || len(p.Save) != 1 {
		t.Errorf("a flag change runs nothing but must be saved: %s %v", p.summary(), p.Save)
	}
	if _, err := planBetween(t, old, new, planOpts{mode: ModeSafe}); err == nil || !strings.Contains(err.Error(), "structural flag") {
		t.Errorf("safe mode should refuse a flag change, got %v", err)
	}
	if _, err := planBetween(t, old, new, planOpts{mode: ModeForce}); err != nil {
		t.Errorf("force mode should accept it: %v", err)
	}
}

func TestPlanSafeMode(t *testing.T) {
	base := files("a", "structural: true\nsql:\n  - create table t (a int)\n", "b", "deps: [a]\nreapply_on: [dep_appended]\nsql:\n"+seeds(1))
	appended := files("a", "structural: true\nsql:\n  - create table t (a int)\n  - alter table t add column c int\n", "b", "deps: [a]\nreapply_on: [dep_appended]\nsql:\n"+seeds(1))
	// An append, and the reapply of a default mutation it triggers, are fine.
	p, err := planBetween(t, base, appended, planOpts{mode: ModeSafe})
	if err != nil {
		t.Fatalf("append should pass in safe mode: %v", err)
	}
	wantPlan(t, p, "down:b(reapply dep_appended:a) up:a(append 1) up:b(reapply dep_appended:a)")

	edited := files("a", "structural: true\nsql:\n  - create table u (a int)\n", "b", "deps: [a]\nsql:\n"+seeds(1))
	if _, err := planBetween(t, base, edited, planOpts{mode: ModeSafe}); err == nil || !strings.Contains(err.Error(), "a changes at statement 1") {
		t.Errorf("an edit should be refused naming mutation and position, got %v", err)
	}
	if _, err := planBetween(t, base, files("a", base["a.yml"]), planOpts{mode: ModeSafe}); err == nil || !strings.Contains(err.Error(), "b is removed") {
		t.Errorf("a removal should be refused, got %v", err)
	}

	// A structural mutation that reapplies (here because the user asked for the trigger) is refused.
	old := files("z", "sql:\n"+seeds(1), "s", "structural: true\ndeps: [z]\nreapply_on: [dep_reapplied]\nsql:\n"+seeds(1))
	new := files("s", "structural: true\nreapply_on: [dep_reapplied]\nsql:\n"+seeds(1))
	if _, err := planBetween(t, old, new, planOpts{mode: ModeSafe}); err == nil {
		t.Error("a structural reapply should be refused in safe mode")
	}
	if _, err := planBetween(t, old, new, planOpts{mode: ModeNormal}); err != nil {
		t.Errorf("normal mode accepts it: %v", err)
	}
}

func settingsOf(digests map[string][]SettingDigest) SettingsFunc {
	return func(m *Mutation, i int) []SettingDigest { return digests[fmt.Sprintf("%s/%d", m.Name, i)] }
}

func TestPlanSettingsChange(t *testing.T) {
	f := files("a", "sql:\n"+seeds(3))
	hash := func(name, value string) SettingDigest {
		return SettingDigest{name, SettingHash("", "a", 2, name, &value)}
	}
	old := settingsOf(map[string][]SettingDigest{"a/1": {hash("dmut.env.X", "1"), hash("dmut.env.Y", "1")}})

	// The value of Y changes: only the tail from that statement is redone.
	now := settingsOf(map[string][]SettingDigest{"a/1": {hash("dmut.env.X", "1"), hash("dmut.env.Y", "2")}})
	wantPlan(t, mustPlan(t, f, f, planOpts{oldSettings: old, newSettings: now}),
		"down:a(tail 1 setting:dmut.env.Y) up:a(tail 1 setting:dmut.env.Y)")

	// The setting becomes unset: the marker differs from any value, even an empty one.
	unset := settingsOf(map[string][]SettingDigest{"a/1": {hash("dmut.env.X", "1"), {"dmut.env.Y", UnsetMarker}}})
	wantPlan(t, mustPlan(t, f, f, planOpts{oldSettings: old, newSettings: unset}),
		"down:a(tail 1 setting:dmut.env.Y) up:a(tail 1 setting:dmut.env.Y)")
	empty := settingsOf(map[string][]SettingDigest{"a/1": {hash("dmut.env.X", "1"), hash("dmut.env.Y", "")}})
	wantPlan(t, mustPlan(t, f, f, planOpts{oldSettings: unset, newSettings: empty}),
		"down:a(tail 1 setting:dmut.env.Y) up:a(tail 1 setting:dmut.env.Y)")

	// An untracked setting leaves no entry, so nothing changes.
	untracked := settingsOf(map[string][]SettingDigest{"a/1": {hash("dmut.env.X", "1")}})
	if p := mustPlan(t, f, f, planOpts{oldSettings: untracked, newSettings: untracked}); !p.Empty() {
		t.Errorf("got %s", p.summary())
	}

	// A new entry, or a reorder, is a change too.
	added := settingsOf(map[string][]SettingDigest{"a/1": {hash("dmut.env.X", "1"), hash("dmut.env.Y", "1"), hash("dmut.env.Z", "1")}})
	wantPlan(t, mustPlan(t, f, f, planOpts{oldSettings: old, newSettings: added}),
		"down:a(tail 1 setting:dmut.env.Z) up:a(tail 1 setting:dmut.env.Z)")
}

func TestPlanSeedBeforeTailDoesNotRunAgain(t *testing.T) {
	old := files("a", "sql:\n  - seed: select 1\n  - seed: select 2\n  - seed: select 3\n")
	new := files("a", "sql:\n  - seed: select 1\n  - seed: select 2\n  - seed: select 33\n")
	wantPlan(t, mustPlan(t, old, new, planOpts{}), "down:a(tail 2) up:a(tail 2)")
}

func TestPlanDataChangeIsAChange(t *testing.T) {
	mk := func(v string) map[string]string {
		return files("a", "sql:\n  - seed: insert into t values ($1)\n    data: [["+v+"]]\n")
	}
	wantPlan(t, mustPlan(t, mk("1"), mk("2"), planOpts{}), "down:a(tail 0) up:a(tail 0)")
}

func TestPlanOrderFollowsTheNewGraph(t *testing.T) {
	// The dependency is reversed between the two versions: the downs follow the new one.
	old := files("a", "sql:\n  - seed: select 1\n", "b", "deps: [a]\nsql:\n  - seed: select 1\n")
	new := files("a", "deps: [b]\nsql:\n  - seed: select 2\n", "b", "sql:\n  - seed: select 2\n")
	wantPlan(t, mustPlan(t, old, new, planOpts{}),
		"down:a(tail 0) down:b(tail 0) up:b(tail 0) up:a(tail 0)")
}

func TestPlanOrderCycleBetweenStoredAndNewDependencies(t *testing.T) {
	// y was stored as depending on x, and x on z. x goes away and z now depends on y:
	// the stored edges and the new one close a cycle y -> x -> z -> y.
	old := files("x", "deps: [z]\nsql:\n"+seeds(1), "y", "deps: [x]\nsql:\n"+seeds(1), "z", "sql:\n"+seeds(1))
	new := files("y", "sql:\n"+seeds(1), "z", "deps: [y]\nsql:\n"+seeds(1))
	_, err := planBetween(t, old, new, planOpts{})
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Errorf("want a cycle error, got %v", err)
	}
}

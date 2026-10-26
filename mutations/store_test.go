package mutations

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/ceymard/dmut/v2/mutations/model"
)

// bootstrap creates dmut's tables by running the ups of its own definition in order.
func bootstrap(t *testing.T, r *Runner) {
	t.Helper()
	p, err := LoadProject()
	if err != nil {
		t.Fatal(err)
	}
	order, err := p.Namespaces[0].Revisions[1].UpOrder()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range order {
		for _, st := range m.Statements {
			if err := r.Exec(context.Background(), st.Up); err != nil {
				t.Fatalf("%s: %v", m.Name, err)
			}
		}
	}
}

func TestDmutDefinition(t *testing.T) {
	p, err := LoadProject()
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Namespaces) != 1 || p.Namespaces[0].Name != DmutNamespace {
		t.Fatalf("unexpected namespaces: %+v", p.Namespaces)
	}
	order, err := p.Namespaces[0].Revisions[1].UpOrder()
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, m := range order {
		names = append(names, m.Name)
	}
	if got := strings.Join(names, " "); got != "dmut dmut.deps dmut.mutations dmut.statements" && got != "dmut dmut.mutations dmut.deps dmut.statements" {
		t.Errorf("order: %s", got)
	}
}

func TestLoadStateOfAFreshDatabase(t *testing.T) {
	r := runner(t)
	state, revision, err := r.LoadState(context.Background(), "x")
	if err != nil || len(state) != 0 || revision != 0 {
		t.Errorf("want nothing, got %v %d %v", state, revision, err)
	}
	if names, err := r.Namespaces(context.Background()); err != nil || len(names) != 0 {
		t.Errorf("want no namespaces, got %v %v", names, err)
	}
}

func str(s string) *string { return &s }

func TestStateRoundTrip(t *testing.T) {
	ctx := context.Background()
	r := runner(t)
	bootstrap(t, r)

	api := &model.RecordedMutation{
		Name: "api", File: "api.yml", Revision: 2, Structural: true, ReapplyOn: []model.Trigger{},
		Statements: []model.RecordedStatement{
			{Kind: model.KindReversible, Up: "create schema api", Down: "drop schema api", Digest: "d1"},
			{Kind: model.KindSeed, Up: "insert into t values ($1)", Digest: "d2\ndmut.env.X\th", Saved: map[string]*string{"dmut.env.X": str("v"), "dmut.env.Y": nil}},
			{Kind: model.KindCheck, Up: "select true", Digest: "d3"},
		},
	}
	view := &model.RecordedMutation{
		Name: "api.view", File: "api.view.yml", Revision: 2,
		Statements: []model.RecordedStatement{{Kind: model.KindReversible, Up: "create view v as select 1", Down: "drop view v", Digest: "d4"}},
		Deps: []model.RecordedDep{
			{Entry: "api.*", ReapplyOn: nil, Matched: []string{"api.a", "api.b"}},
			{Entry: "other", ReapplyOn: []model.Trigger{model.DepAppended}, Matched: []string{}},
			{Entry: "none", ReapplyOn: []model.Trigger{}, Matched: []string{}},
		},
		ReapplyOn: []model.Trigger{model.DepsGrow, model.DepsShrink},
	}
	for _, rec := range []*model.RecordedMutation{api, view} {
		if err := r.SaveMutation(ctx, "ns", rec); err != nil {
			t.Fatal(err)
		}
		if err := r.InsertStatements(ctx, "ns", rec.Name, 0, rec.Statements); err != nil {
			t.Fatal(err)
		}
	}

	state, revision, err := r.LoadState(ctx, "ns")
	if err != nil {
		t.Fatal(err)
	}
	if revision != 2 || len(state) != 2 {
		t.Fatalf("revision %d, %d mutations", revision, len(state))
	}
	for _, want := range []*model.RecordedMutation{api, view} {
		if got := state[want.Name]; !reflect.DeepEqual(got, want) {
			t.Errorf("%s did not round trip:\n got  %+v\n want %+v", want.Name, got, want)
		}
	}
	if other, _, _ := r.LoadState(ctx, "other"); len(other) != 0 {
		t.Error("another namespace should be empty")
	}
	if names, _ := r.Namespaces(ctx); !reflect.DeepEqual(names, []string{"ns"}) {
		t.Errorf("namespaces: %v", names)
	}
}

func TestTailReplaceAndDelete(t *testing.T) {
	ctx := context.Background()
	r := runner(t)
	bootstrap(t, r)

	stmts := func(ups ...string) []model.RecordedStatement {
		var out []model.RecordedStatement
		for _, u := range ups {
			out = append(out, model.RecordedStatement{Kind: model.KindSeed, Up: u, Digest: "d"})
		}
		return out
	}
	rec := &model.RecordedMutation{Name: "m", File: "m.yml", Revision: 1, Statements: stmts("a", "b", "c")}
	if err := r.SaveMutation(ctx, "ns", rec); err != nil {
		t.Fatal(err)
	}
	if err := r.InsertStatements(ctx, "ns", "m", 0, rec.Statements); err != nil {
		t.Fatal(err)
	}

	// Replace the tail from the second statement, then append one.
	if err := r.DeleteStatementsFrom(ctx, "ns", "m", 1); err != nil {
		t.Fatal(err)
	}
	if err := r.InsertStatements(ctx, "ns", "m", 1, stmts("B", "C")); err != nil {
		t.Fatal(err)
	}
	if err := r.InsertStatements(ctx, "ns", "m", 3, stmts("D")); err != nil {
		t.Fatal(err)
	}
	state, _, _ := r.LoadState(ctx, "ns")
	var ups []string
	for _, s := range state["m"].Statements {
		ups = append(ups, s.Up)
	}
	if got := strings.Join(ups, ""); got != "aBCD" {
		t.Errorf("statements: %s", got)
	}

	// Deleting the mutation takes its statements and dependencies with it.
	if err := r.DeleteMutations(ctx, "ns", []string{"m"}); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := r.conn.QueryRow(ctx, `select (select count(*) from __dmut__.statements) + (select count(*) from __dmut__.deps)`).Scan(&left); err != nil || left != 0 {
		t.Errorf("rows left: %d %v", left, err)
	}
}

func TestSaveMutationReplacesDependencies(t *testing.T) {
	ctx := context.Background()
	r := runner(t)
	bootstrap(t, r)
	rec := &model.RecordedMutation{Name: "m", File: "m.yml", Revision: 1, Deps: []model.RecordedDep{{Entry: "a", Matched: []string{"a"}}, {Entry: "b", Matched: []string{"b"}}}}
	if err := r.SaveMutation(ctx, "ns", rec); err != nil {
		t.Fatal(err)
	}
	rec.Deps = rec.Deps[1:]
	rec.Structural = true
	if err := r.SaveMutation(ctx, "ns", rec); err != nil {
		t.Fatal(err)
	}
	state, _, _ := r.LoadState(ctx, "ns")
	if got := state["m"]; len(got.Deps) != 1 || got.Deps[0].Entry != "b" || !got.Structural {
		t.Errorf("unexpected record: %+v", got)
	}
}

func TestOldFormatIsRefused(t *testing.T) {
	ctx := context.Background()
	r := runner(t)
	if err := r.Exec(ctx, `create schema __dmut__; create table __dmut__.mutations (name text, core text[]); create table __dmut__.statements ()`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.LoadState(ctx, "x"); err == nil || !strings.Contains(err.Error(), "earlier version") {
		t.Errorf("want a refusal, got %v", err)
	}
}

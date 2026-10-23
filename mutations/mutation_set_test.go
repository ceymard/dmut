package mutations

import (
	"strings"
	"testing"
)

func stmt(up string) MutationStatement {
	return MutationStatement{Up: up, Down: ""}
}

func stmts(ups ...string) []MutationStatement {
	var res []MutationStatement
	for _, up := range ups {
		res = append(res, stmt(up))
	}
	return res
}

func TestCommonPrefixLenIdenticalLists(t *testing.T) {
	old := stmts("create table t (id int);", "alter table t add column c text;")
	new_ := stmts("create table t (id int);", "alter table t add column c text;")

	if got := commonPrefixLen(old, new_); got != 2 {
		t.Errorf("expected the whole list to match, got prefix length %d", got)
	}
}

// A pure append (nothing before it changed) must not be swept into a down/up tail -
// this is the whole point of the longest-common-prefix diff (see specs/changes.md,
// "Why").
func TestCommonPrefixLenPureAppend(t *testing.T) {
	old := stmts("create table t (id int);")
	new_ := stmts("create table t (id int);", "create index t_idx on t (id);")

	got := commonPrefixLen(old, new_)
	if got != 1 {
		t.Errorf("expected the common prefix to cover the unchanged statement, got %d", got)
	}
	old_tail := old[got:]
	new_tail := new_[got:]
	if len(old_tail) != 0 {
		t.Errorf("a pure append must have an empty old tail, got %d statements", len(old_tail))
	}
	if len(new_tail) != 1 {
		t.Errorf("expected exactly the appended statement in the new tail, got %d", len(new_tail))
	}
}

// Editing a statement in the middle sweeps everything positionally after it into
// both tails, even statements whose own text never changed - this is required for
// correctness (see specs/changes.md, "New algorithm"), not just simplicity.
func TestCommonPrefixLenEditInTheMiddleSweepsTheRest(t *testing.T) {
	old := stmts(
		"create table t (id int);",
		"alter table t add column foo text;",
		"alter table t rename column foo to bar;",
	)
	new_ := stmts(
		"create table t (id int);",
		"alter table t add column foo2 text;",
		"alter table t rename column foo to bar;", // unedited, now stale
	)

	got := commonPrefixLen(old, new_)
	if got != 1 {
		t.Fatalf("expected the prefix to stop right before the edited statement, got %d", got)
	}
	if len(old[got:]) != 2 || len(new_[got:]) != 2 {
		t.Errorf("expected both tails to cover everything after the edit, including the "+
			"unedited rename, got old=%d new=%d", len(old[got:]), len(new_[got:]))
	}
}

// Whitespace and comments are normalized away before comparison - see DigestBuffer.
func TestCommonPrefixLenIgnoresWhitespaceAndComments(t *testing.T) {
	old := stmts("create   table t (id int);")
	new_ := stmts("-- a comment\ncreate table t (id int);\n")

	if got := commonPrefixLen(old, new_); got != 1 {
		t.Errorf("expected whitespace/comment-only differences to compare equal, got prefix length %d", got)
	}
}

// Inserting a brand-new statement in the middle is a different shape from editing one
// in place: everything positionally after the insertion point shifts, so even a
// statement whose *text* is byte-identical on both sides (it just moved one slot to
// the right) must still be swept into both tails - the LCP diff has no way to notice
// "this is actually still here, just later," and re-running it in place (rather than
// detecting a no-op) is the only sound thing it can do without a general diff.
func TestCommonPrefixLenInsertionInTheMiddleShiftsEverythingAfter(t *testing.T) {
	old := stmts(
		"create table t (id int);",
		"alter table t add column bar text;",
	)
	new_ := stmts(
		"create table t (id int);",
		"alter table t add column foo text;", // newly inserted
		"alter table t add column bar text;", // byte-identical to old[1], just shifted
	)

	got := commonPrefixLen(old, new_)
	if got != 1 {
		t.Fatalf("expected the prefix to stop right before the insertion, got %d", got)
	}
	old_tail := old[got:]
	new_tail := new_[got:]
	if len(old_tail) != 1 {
		t.Errorf("expected the old tail to still cover the shifted statement, got %d", len(old_tail))
	}
	if len(new_tail) != 2 {
		t.Errorf("expected the new tail to cover both the insertion and the shifted statement, got %d", len(new_tail))
	}
}

func TestCommonPrefixLenEmptyLists(t *testing.T) {
	if got := commonPrefixLen(nil, nil); got != 0 {
		t.Errorf("expected 0 for two empty lists, got %d", got)
	}
	if got := commonPrefixLen(nil, stmts("create schema x;")); got != 0 {
		t.Errorf("expected 0 when there's nothing to compare against, got %d", got)
	}
}

const lcpBase = `
api:
  core:
    - create schema api;

api.base:
  core:
    - create table api.base (id int);
  glue:
    - create function api.f() returns int as $$ select 1 $$ language sql;
    - create function api.g() returns int as $$ select 2 $$ language sql;

api.report:
  needs: [api.base]
  glue_needs: [api.base]
  glue:
    - create view api.report as select api.g() as x;

api.summary:
  needs: [api.report]
  glue_needs: [api.report]
  glue:
    - create view api.summary as select x + 1 as y from api.report;

api.unrelated:
  glue:
    - create function api.u() returns int as $$ select 0 $$ language sql;
`

// lcpDelta loads both versions and returns what going from old to new would run.
func lcpDelta(t *testing.T, old, new_ string, dir IterationDirection) (down, up *RunnableMap, real_down bool) {
	t.Helper()

	old_set := set(t, mustLoad(t, map[string]string{"a.yml": old}), "")
	new_set := set(t, mustLoad(t, map[string]string{"a.yml": new_}), "")
	return new_set.GetLCPDelta(old_set, dir)
}

func sizes(rm *RunnableMap) map[string]int {
	res := map[string]int{}
	for _, name := range rm.Keys() {
		runnable, _ := rm.Get(name)
		res[name] = runnable.Size()
	}
	return res
}

func requireOrder(t *testing.T, what string, got, want []string) {
	t.Helper()

	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("%s: expected %v, got %v", what, want, got)
	}
}

// Editing the first glue statement downs the whole glue tail of api.base. Whatever
// depends on api.base is downed first and in full, and comes back up after it.
func TestLCPGlueTailCascadesToDependents(t *testing.T) {
	edited := strings.Replace(lcpBase, "select 1 $$", "select 11 $$", 1)

	down, up, real_down := lcpDelta(t, lcpBase, edited, ITER_META)

	if !real_down {
		t.Error("editing a statement is a real down")
	}
	requireOrder(t, "down order", down.Keys(), []string{"api.summary", "api.report", "api.base"})
	requireOrder(t, "up order", up.Keys(), []string{"api.base", "api.report", "api.summary"})

	want := map[string]int{"api.summary": 1, "api.report": 1, "api.base": 2}
	for name, size := range want {
		if got := sizes(down)[name]; got != size {
			t.Errorf("down of %s should run %d statements, got %d", name, size, got)
		}
		if got := sizes(up)[name]; got != size {
			t.Errorf("up of %s should run %d statements, got %d", name, size, got)
		}
	}
}

// A dependent that was edited too is rebuilt once, from its new definition.
func TestLCPGlueDependentThatChangedIsRebuiltInFull(t *testing.T) {
	edited := strings.Replace(lcpBase, "select 1 $$", "select 11 $$", 1)
	edited = strings.Replace(edited, "select api.g() as x", "select api.g() * 10 as x", 1)

	down, up, _ := lcpDelta(t, lcpBase, edited, ITER_META)

	requireOrder(t, "down order", down.Keys(), []string{"api.summary", "api.report", "api.base"})
	requireOrder(t, "up order", up.Keys(), []string{"api.base", "api.report", "api.summary"})

	runnable, _ := up.Get("api.report")
	if !strings.Contains(runnable.Mutation.Glue[0].Up, "* 10") {
		t.Error("api.report should come back up from its new definition")
	}
}

// Appending downs nothing, so nothing that depends on it is touched.
func TestLCPGlueAppendLeavesDependentsAlone(t *testing.T) {
	appended := strings.Replace(lcpBase,
		"    - create function api.g() returns int as $$ select 2 $$ language sql;\n",
		"    - create function api.g() returns int as $$ select 2 $$ language sql;\n    - create function api.h() returns int as $$ select 3 $$ language sql;\n", 1)

	down, up, real_down := lcpDelta(t, lcpBase, appended, ITER_META)

	if real_down {
		t.Error("an append is not a real down")
	}
	if down.Size() != 0 {
		t.Errorf("nothing should come down, got %v", down.Keys())
	}
	requireOrder(t, "up", up.Keys(), []string{"api.base"})
	if got := sizes(up)["api.base"]; got != 1 {
		t.Errorf("only the appended statement should run, got %d", got)
	}
}

// Mutations that nothing links to the edited one are left alone.
func TestLCPGlueUnrelatedMutationsAreLeftAlone(t *testing.T) {
	edited := strings.Replace(lcpBase, "select 1 $$", "select 11 $$", 1)

	down, up, _ := lcpDelta(t, lcpBase, edited, ITER_META)

	for _, rm := range []*RunnableMap{down, up} {
		if _, found := rm.Get("api.unrelated"); found {
			t.Error("api.unrelated does not depend on api.base and must not be rebuilt")
		}
	}
}

// Core never cascades on its own : rebuilding a dependent table would throw its data away,
// which only `full` mode may do.
func TestLCPCoreTailDoesNotCascade(t *testing.T) {
	edited := strings.Replace(lcpBase, "create table api.base (id int);", "create table api.base (id bigint);", 1)

	down, up, real_down := lcpDelta(t, lcpBase, edited, ITER_SQL)

	if !real_down {
		t.Error("editing core is a real down")
	}
	requireOrder(t, "down", down.Keys(), []string{"api.base"})
	requireOrder(t, "up", up.Keys(), []string{"api.base"})
}

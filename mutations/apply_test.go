package mutations

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ceymard/dmut/v3/mutations/model"
	"github.com/jackc/pgx/v5"
)

// harness applies sets of files to one database and watches what ran.
type harness struct {
	t   *testing.T
	uri string
	rec *recorder
}

func newHarness(t *testing.T) *harness {
	return &harness{t: t, uri: testdb(t), rec: newRecorder()}
}

// writeFiles writes files, keyed by name without extension, in a fresh directory and returns it as the path list.
func writeFiles(t *testing.T, files map[string]string) []string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name+".yml"), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return []string{dir}
}

// apply applies files, committing unless opts say otherwise.
func (h *harness) apply(files map[string]string, mods ...func(*Options)) error {
	h.t.Helper()
	paths := writeFiles(h.t, files)
	*h.rec.out = nil
	opts := Options{Commit: true, Logger: slog.New(h.rec)}
	for _, mod := range mods {
		mod(&opts)
	}
	return Apply(context.Background(), h.uri, paths, opts)
}

func (h *harness) mustApply(files map[string]string, mods ...func(*Options)) {
	h.t.Helper()
	if err := h.apply(files, mods...); err != nil {
		h.t.Fatalf("apply failed: %v", err)
	}
}

// ran lists what ran, as "down:b up:a(dep_reapplied:b)", ignoring dmut's own namespace.
func (h *harness) ran() string {
	var parts []string
	for _, rec := range *h.rec.out {
		if rec["msg"] != "run" || rec["namespace"] == DmutNamespace {
			continue
		}
		s := fmt.Sprintf("%s:%s", rec["dir"], rec["mutation"])
		if reason, ok := rec["reason"]; ok {
			s += fmt.Sprintf("(%s:%s)", reason, rec["cause"])
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, " ")
}

// query returns the rows of sql as text, columns joined by "," and rows by ";".
func (h *harness) query(sql string, args ...any) string {
	h.t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, h.uri)
	if err != nil {
		h.t.Fatal(err)
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			h.t.Fatal(err)
		}
		cols := make([]string, len(values))
		for i, v := range values {
			cols[i] = fmt.Sprint(v)
		}
		out = append(out, strings.Join(cols, ","))
	}
	if err := rows.Err(); err != nil {
		h.t.Fatalf("%s: %v", sql, err)
	}
	return strings.Join(out, ";")
}

func (h *harness) want(sql, want string) {
	h.t.Helper()
	if got := h.query(sql); got != want {
		h.t.Errorf("%s\n got  %q\n want %q", sql, got, want)
	}
}

func (h *harness) wantRan(want string) {
	h.t.Helper()
	if got := h.ran(); got != want {
		h.t.Errorf("what ran:\n got  %s\n want %s", got, want)
	}
}

func mode(m model.Mode) func(*Options) { return func(o *Options) { o.Mode = m } }

func f(pairs ...string) map[string]string {
	out := map[string]string{}
	for i := 0; i < len(pairs); i += 2 {
		out[pairs[i]] = pairs[i+1]
	}
	return out
}

func TestApplyBootstrapsAndRecords(t *testing.T) {
	h := newHarness(t)
	files := f("t", "structural: true\nsql:\n  - create table t (a int)\n  - alter table t add column b int\n")
	h.mustApply(files)
	h.wantRan("up:t")
	h.want("select count(*) from information_schema.columns where table_name = 't'", "2")
	h.want("select name, structural, revision from __dmut__.mutations where namespace = ''", "t,true,1")
	h.want("select position, kind from __dmut__.statements where namespace = '' order by position", "1,reversible;2,reversible")

	// A second run finds nothing to do.
	h.mustApply(files)
	h.wantRan("")
}

func TestApplyAppendThenTailThenRemoval(t *testing.T) {
	h := newHarness(t)
	v1 := "structural: true\nsql:\n  - create table t (a int)\n"
	h.mustApply(f("t", v1))
	h.query("insert into t values (1)") // pgx runs it; the row must survive an append

	// Append: nothing goes down.
	h.mustApply(f("t", v1+"  - alter table t add column b int default 5\n"))
	h.wantRan("up:t")
	h.want("select a, b from t", "1,5")

	// Edit of the last statement: the tail goes down and up.
	h.mustApply(f("t", v1+"  - alter table t add column c int default 7\n"))
	h.wantRan("down:t up:t")
	h.want("select a, c from t", "1,7")
	h.want("select count(*) from information_schema.columns where table_name = 't' and column_name = 'b'", "0")

	// Removal.
	h.mustApply(f("other", "sql:\n  - seed: select 1\n"))
	h.wantRan("down:t up:other")
	h.want("select to_regclass('t') is null", "true")
	h.want("select count(*) from __dmut__.mutations where namespace = '' and name = 't'", "0")
}

func TestApplyReorderRedoesTheTail(t *testing.T) {
	h := newHarness(t)
	base := "structural: true\nsql:\n  - create table t (a int)\n"
	h.mustApply(f("t", base+"  - alter table t add column b int\n  - alter table t add column c int\n"))
	h.mustApply(f("t", base+"  - alter table t add column c int\n  - alter table t add column b int\n"))
	h.wantRan("down:t up:t")
	h.want("select string_agg(column_name, ',' order by ordinal_position) from information_schema.columns where table_name = 't'", "a,c,b")
}

func TestApplyDependencyTriggers(t *testing.T) {
	h := newHarness(t)
	table := "structural: true\nsql:\n  - create table t (a int)\n"
	view := "deps: [t]\nreapply_on: [dep_appended]\nsql:\n  - create view v as select * from t\n"
	h.mustApply(f("t", table, "v", view))

	// A column is appended to the table: the view is reapplied and sees it.
	h.mustApply(f("t", table+"  - alter table t add column b int\n", "v", view))
	h.wantRan("down:v(dep_appended:t) up:t up:v(dep_appended:t)")
	h.want("select string_agg(column_name, ',' order by ordinal_position) from information_schema.columns where table_name = 'v'", "a,b")
}

func TestApplyCascadeOnRemovedDependency(t *testing.T) {
	h := newHarness(t)
	h.mustApply(f(
		"base", "sql:\n  - create function f() returns int language sql as 'select 1'\n",
		"user", "deps: [base]\nsql:\n  - create view v as select f() as x\n"))
	// base goes away and user stops listing it: user, a default mutation, is reapplied, and goes down first.
	h.mustApply(f("user", "sql:\n  - create view v as select 1 as x\n"))
	h.wantRan("down:user(dep_reapplied:base) down:base up:user(dep_reapplied:base)")
	h.want("select x from v", "1")
	h.want("select to_regproc('f') is null", "true")
}

func TestApplyStatementErrorRollsEverythingBack(t *testing.T) {
	h := newHarness(t)
	err := h.apply(f("t", "structural: true\nsql:\n  - create table t (a int)\n  - alter table t add column b nonsense\n"))
	if err == nil || !strings.Contains(err.Error(), "nonsense") {
		t.Fatalf("want the postgres error, got %v", err)
	}
	h.want("select to_regclass('t') is null", "true")
	h.want("select to_regclass('__dmut__.mutations') is null", "true")
}

func TestApplyDryRunRollsBack(t *testing.T) {
	h := newHarness(t)
	h.mustApply(f("t", "structural: true\nsql:\n  - create table t (a int)\n"), func(o *Options) { o.Commit = false })
	h.want("select to_regclass('t') is null", "true")
}

func TestApplySafeModeRefusesBeforeRunningAnything(t *testing.T) {
	h := newHarness(t)
	h.mustApply(f("t", "structural: true\nsql:\n  - create table t (a int)\n", "u", "sql:\n  - seed: select 1\n"))
	err := h.apply(f("t", "structural: true\nsql:\n  - create table t2 (a int)\n", "u", "sql:\n  - seed: select 2\n  - seed: select 3\n"), mode(model.ModeSafe))
	if err == nil || !strings.Contains(err.Error(), "t changes at statement 1") {
		t.Fatalf("want a refusal, got %v", err)
	}
	h.want("select to_regclass('t') is not null and to_regclass('t2') is null", "true")
	// An append alone is fine.
	h.mustApply(f("t", "structural: true\nsql:\n  - create table t (a int)\n  - alter table t add column b int\n", "u", "sql:\n  - seed: select 1\n"), mode(model.ModeSafe))
}

func TestApplyForceModeFallsBackToReapplyingDependents(t *testing.T) {
	h := newHarness(t)
	fn := func(body string) string {
		return "sql:\n  - up: create function f() returns int language sql as 'select " + body + "'\n    down: drop function f()\n"
	}
	view := "deps: [f]\nsql:\n  - create view v as select f() as x\n"
	h.mustApply(f("f", fn("1"), "v", view))

	// Without -f the drop of f fails because v needs it.
	if err := h.apply(f("f", fn("2"), "v", view)); err == nil {
		t.Fatal("expected the normal mode to fail")
	}
	h.want("select x from v", "1")

	h.mustApply(f("f", fn("2"), "v", view), mode(model.ModeForce))
	h.wantRan("down:f down:v(dep_reapplied:f) down:f up:f up:v(dep_reapplied:f)") // the first attempt, then the retry
	h.want("select x from v", "2")
}

func TestApplyOverrideAdoptsExistingObjects(t *testing.T) {
	h := newHarness(t)
	h.mustApply(f()) // dmut's own tables first
	h.query("create table t (a int)")
	files := f("t", "structural: true\nsql:\n  - create table t (a int)\n")
	h.mustApply(files, func(o *Options) { o.Override = true })
	h.want("select count(*) from __dmut__.statements where namespace = '' and mutation = 't'", "1")
	// What is recorded counts as applied.
	h.mustApply(files)
	h.wantRan("")
}

func TestApplyOverrideOfMissingObjectsFails(t *testing.T) {
	h := newHarness(t)
	err := h.apply(f("t", "structural: true\nsql:\n  - create table t (a int)\n"), func(o *Options) { o.Override = true })
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("the test phase should refuse to record what is not there, got %v", err)
	}
}

func TestApplyRevisionsWithDeleteAndOverride(t *testing.T) {
	h := newHarness(t)
	h.mustApply(f(
		"r1", "revision: 1\nnamespace: x\nmutations:\n  t:\n    structural: true\n    sql:\n      - create table t (a int)\n",
	))
	h.mustApply(f(
		"r1", "revision: 1\nnamespace: x\nmutations:\n  t:\n    structural: true\n    sql:\n      - create table t (a int)\n",
		"r2", `revision: 2
namespace: x
mutations:
  t:
    structural: true
    sql:
      - create table t (a int)
      - alter table t add column b int
  t.c:
    structural: true
    sql:
      - alter table t add column c int
override:
  t:
    structural: true
    sql:
      - create table t (a int, b int, c int)
delete: [t.c]
`))
	h.want("select string_agg(column_name, ',' order by ordinal_position) from information_schema.columns where table_name = 't'", "a,b,c")
	h.want("select name, revision from __dmut__.mutations where namespace = 'x'", "t,2")
	h.want("select up from __dmut__.statements where namespace = 'x'", "create table t (a int, b int, c int)")
}

func TestApplyOldFormatDatabaseIsRefused(t *testing.T) {
	h := newHarness(t)
	h.query("select 1")
	conn, err := pgx.Connect(context.Background(), h.uri)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(context.Background(), `create schema __dmut__; create table __dmut__.mutations (name text, core text[])`); err != nil {
		t.Fatal(err)
	}
	conn.Close(context.Background())
	if err := h.apply(f("t", "sql:\n  - seed: select 1\n")); err == nil || !strings.Contains(err.Error(), "earlier version") {
		t.Errorf("want a refusal, got %v", err)
	}
}

package mutations

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ceymard/dmut/v2/mutations/model"
)

const (
	apiSchema  = "structural: true\nsql:\n  - create schema api\n"
	apiUsers   = "structural: true\ndeps: [api]\nsql:\n  - create table api.users (id serial primary key, name text not null)\n"
	usersGrant = "deps: [api.users]\nsql:\n  - grant select on table api.users to public\n"
)

func tableExists(h *harness, name string) bool {
	return h.query("select to_regclass('"+name+"') is not null") == "true"
}

func TestNamespacesAreIndependent(t *testing.T) {
	h := newHarness(t)
	files := f("api", "namespace: one\n"+apiSchema, "blog", "namespace: two\nstructural: true\nsql:\n  - create schema blog\n")
	h.mustApply(files)
	h.want("select count(*) from information_schema.schemata where schema_name in ('api', 'blog')", "2")

	r, err := NewRunner(context.Background(), h.uri, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	if err := r.DownNamespace(context.Background(), "two", true); err != nil {
		t.Fatal(err)
	}
	h.want("select count(*) from information_schema.schemata where schema_name = 'blog'", "0")
	h.want("select count(*) from information_schema.schemata where schema_name = 'api'", "1")
	h.want("select namespace from __dmut__.mutations where namespace <> '__dmut__'", "one")
}

func TestDownRemovesEverythingInReverseOrder(t *testing.T) {
	h := newHarness(t)
	h.mustApply(f("api", apiSchema, "api.users", apiUsers, "api.users.grant", usersGrant))
	r, err := NewRunner(context.Background(), h.uri, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	if err := r.DownNamespace(context.Background(), "", true); err != nil {
		t.Fatal(err)
	}
	h.want("select count(*) from information_schema.schemata where schema_name = 'api'", "0")
	h.want("select count(*) from __dmut__.mutations where namespace = ''", "0")
	if err := r.DownNamespace(context.Background(), DmutNamespace, true); err == nil {
		t.Error("dmut's own namespace must not go down")
	}
}

func TestDownDryRunChangesNothing(t *testing.T) {
	h := newHarness(t)
	h.mustApply(f("api", apiSchema))
	r, err := NewRunner(context.Background(), h.uri, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	if err := r.DownNamespace(context.Background(), "", false); err != nil {
		t.Fatal(err)
	}
	h.want("select count(*) from information_schema.schemata where schema_name = 'api'", "1")
}

// revisionFiles returns two revisions of one namespace: r2 adds a column in a child mutation,
// inserts a row, and squashes both into one consolidated definition.
func revisionFiles() map[string]string {
	r1 := "revision: 1\nnamespace: x\nmutations:\n  t:\n    structural: true\n    sql:\n      - create table t (name text)\n"
	r2 := `revision: 2
namespace: x
mutations:
  t:
    structural: true
    sql:
      - create table t (name text)
  t.age:
    structural: true
    sql:
      - alter table t add column age int
      - seed: insert into t (name) values ('from r2')
override:
  t:
    structural: true
    sql:
      - create table t (name text, age int)
delete: [t.age]
`
	return f("r1", r1, "r2", r2)
}

func TestEmptyDatabaseGetsOnlyTheLatestRevision(t *testing.T) {
	h := newHarness(t)
	h.mustApply(revisionFiles())
	h.want("select distinct revision from __dmut__.mutations where namespace = 'x'", "2")
	h.want("select count(*) from __dmut__.mutations where namespace = 'x'", "1")
	h.want("select count(*) from t", "1") // r2 runs for real: it is the one applied
}

func TestRevisionsKeepTheData(t *testing.T) {
	h := newHarness(t)
	all := revisionFiles()
	h.mustApply(f("r1", all["r1"]))
	h.want("select distinct revision from __dmut__.mutations where namespace = 'x'", "1")
	h.query("insert into t values ('written at revision 1')")

	h.mustApply(all)
	h.want("select distinct revision from __dmut__.mutations where namespace = 'x'", "2")
	h.want("select count(*) from t where name = 'written at revision 1'", "1")
	h.want("select count(*) from t where name = 'from r2'", "1")
	h.want("select count(*) from information_schema.columns where table_name = 't' and column_name = 'age'", "1")
	h.want("select up from __dmut__.statements where namespace = 'x'", "create table t (name text, age int)") // the override is what is recorded

	// An up to date database is left alone.
	h.mustApply(all)
	h.wantRan("")
}

func TestReplayingAllRevisions(t *testing.T) {
	h := newHarness(t)
	h.mustApply(revisionFiles(), func(o *Options) { o.All = true })
	h.want("select distinct revision from __dmut__.mutations where namespace = 'x'", "2")
	h.want("select count(*) from t where name = 'from r2'", "1")
}

func TestRevisionDeleteDoesNotDownThePhysicalObject(t *testing.T) {
	h := newHarness(t)
	r1 := "revision: 1\nnamespace: x\nmutations:\n  api:\n" + indentBlock(apiSchema) + "  api.users:\n" + indentBlock(apiUsers)
	r2 := "revision: 2\nnamespace: x\ndelete: [api.users]\nmutations:\n  api:\n" + indentBlock(apiSchema) + "  api.users:\n" + indentBlock(apiUsers)
	h.mustApply(f("r1", r1))
	h.mustApply(f("r1", r1, "r2", r2))
	if !tableExists(h, "api.users") {
		t.Error("delete: must not down the table")
	}
	h.want("select name from __dmut__.mutations where namespace = 'x'", "api")
}

func indentBlock(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		b.WriteString("    " + line + "\n")
	}
	return b.String()
}

func TestDeleteWinsOverOverrideForTheSameName(t *testing.T) {
	h := newHarness(t)
	h.mustApply(f("r1", "revision: 1\nnamespace: x\nmutations:\n  api:\n"+indentBlock(apiSchema)))
	h.mustApply(f("r2", "revision: 2\nnamespace: x\ndelete: [api]\nmutations:\n  api:\n"+indentBlock(apiSchema)+"override:\n  api:\n"+indentBlock(apiSchema)))
	h.want("select count(*) from __dmut__.mutations where namespace = 'x'", "0")
	h.want("select count(*) from information_schema.schemata where schema_name = 'api'", "1")
}

func TestOrphanOverrideAdoptsAnExistingObject(t *testing.T) {
	h := newHarness(t)
	h.mustApply(f("r1", "revision: 1\nnamespace: x\nmutations:\n  api:\n"+indentBlock(apiSchema)))
	h.query("create table api.audit (id int)")
	h.query("insert into api.audit values (1)")
	r2 := "revision: 2\nnamespace: x\nmutations:\n  api:\n" + indentBlock(apiSchema) +
		"override:\n  api.audit:\n    structural: true\n    sql:\n      - create table api.audit (id int)\n"
	h.mustApply(f("r1", "revision: 1\nnamespace: x\nmutations:\n  api:\n"+indentBlock(apiSchema), "r2", r2))
	h.want("select count(*) from api.audit", "1") // never executed
	h.want("select count(*) from __dmut__.mutations where namespace = 'x'", "2")

	// An orphan for something that does not exist is found by the test phase.
	h2 := newHarness(t)
	err := h2.apply(f("r2", r2))
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("want the missing object to be found, got %v", err)
	}
	h2.want("select to_regclass('__dmut__.mutations') is null", "true")
}

func TestRevisionModeWinsOverTheOption(t *testing.T) {
	h := newHarness(t)
	v1 := "structural: true\nsql:\n  - create table t (a int)\n"
	h.mustApply(f("t", v1))
	changed := "revision: 2\nnamespace: \"\"\nmode: force\nmutations:\n  t:\n    structural: true\n    sql:\n      - create table t2 (a int)\n"
	// Safe mode alone refuses this edit; the revision asks for force.
	h.mustApply(f("t", v1))
	if err := h.apply(f("r2", changed), mode(model.ModeSafe)); err != nil {
		t.Fatalf("the mode of the revision should win over safe: %v", err)
	}
	h.want("select to_regclass('t2') is not null", "true")
}

func TestApplyWithCancelledContextFails(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Apply(ctx, h.uri, writeFiles(t, f("api", apiSchema)), Options{Commit: true})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestDeadlineMidRunLeavesNothingBehind(t *testing.T) {
	h := newHarness(t)
	paths := writeFiles(t, f("slow", "structural: true\nsql:\n  - create schema slow\n  - up: select pg_sleep(60)\n    down: select 1\n"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	err := Apply(ctx, h.uri, paths, Options{Commit: true})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want context.DeadlineExceeded, got %v", err)
	}
	if time.Since(start) > 20*time.Second {
		t.Error("the run should stop at the deadline")
	}
	h.want("select count(*) from information_schema.schemata where schema_name = 'slow'", "0")
	deadline := time.Now().Add(5 * time.Second)
	for {
		busy := h.query(`select count(*) from pg_stat_activity where datname = current_database() and pid <> pg_backend_pid() and (query like '%pg_sleep%' or state like 'idle in transaction%')`)
		if busy == "0" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s backend(s) still busy after the run returned", busy)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

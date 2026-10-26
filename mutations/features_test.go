package mutations

import (
	"fmt"
	"strings"
	"testing"
)

const logTable = "structural: true\nsql:\n  - create table log (msg text)\n"

func TestSeedRunsOnUpNotOnDownAndNotBeforeTheTail(t *testing.T) {
	h := newHarness(t)
	seeds := func(last string) string {
		return "structural: true\ndeps: [log]\nsql:\n  - seed: insert into log values ('first')\n  - up: create table made (a int)\n    down: drop table made\n  - seed: insert into log values ('" + last + "')\n"
	}
	h.mustApply(f("log", logTable, "s", seeds("a")))
	h.want("select string_agg(msg, ',' order by ctid) from log", "first,a")

	// Only the tail from the changed statement goes back up: the first seed does not run again.
	h.mustApply(f("log", logTable, "s", seeds("b")))
	h.want("select string_agg(msg, ',' order by ctid) from log", "first,a,b")

	// A seed has no down: going down only drops the table.
	h.mustApply(f("log", logTable))
	h.want("select string_agg(msg, ',' order by ctid) from log", "first,a,b")
	h.want("select to_regclass('made') is null", "true")
}

func TestSeedRunsAgainOnReapply(t *testing.T) {
	h := newHarness(t)
	fn := func(n string) string {
		return "sql:\n  - up: create function f() returns int language sql as 'select " + n + "'\n    down: drop function f()\n"
	}
	s := "deps: [f]\nreapply_on: [dep_reapplied, dep_appended]\nsql:\n  - seed: insert into log values ('seeded')\n"
	h.mustApply(f("log", logTable, "f", fn("1"), "s", s+""))
	h.want("select count(*) from log", "1")
	h.mustApply(f("log", logTable, "f", fn("1")+"  - seed: select 1\n", "s", s))
	h.want("select count(*) from log", "2")
}

func TestSeedData(t *testing.T) {
	h := newHarness(t)
	h.mustApply(f("roles", `structural: true
sql:
  - create table roles (id int primary key, name text not null, tags jsonb, born date, weight numeric)
  - seed: insert into roles values ($1, $2, $3, $4, $5) on conflict (id) do nothing
    data:
      - [1, admin, '["a", "b"]', 2020-01-31, 1.50]
      - [2, guest, null, null, 3]
      - [3, mapper, {a: [1, 2]}, null, null]
`))
	h.want("select id, name, coalesce(tags::text, '-'), coalesce(born::text, '-'), coalesce(weight::text, '-') from roles order by id",
		`1,admin,["a", "b"],2020-01-31,1.5;2,guest,-,-,3;3,mapper,{"a": [1, 2]},-,-`)
}

func TestSeedDataChangeRedoesTheTail(t *testing.T) {
	h := newHarness(t)
	mk := func(v string) map[string]string {
		return f("t", "structural: true\nsql:\n  - create table t (a int)\n  - seed: insert into t values ($1)\n    data: [["+v+"]]\n")
	}
	h.mustApply(mk("1"))
	h.mustApply(mk("2"))
	h.wantRan("down:t up:t")
	h.want("select string_agg(a::text, ',' order by a) from t", "1,2") // a seed is never undone
}

func TestSeedDataBadValueNamesTheStatement(t *testing.T) {
	h := newHarness(t)
	err := h.apply(f("t", "structural: true\nsql:\n  - create table t (a int)\n  - seed: insert into t values ($1)\n    data: [[1], [nope]]\n"))
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Errorf("want the postgres error, got %v", err)
	}
}

func TestChecks(t *testing.T) {
	h := newHarness(t)
	table := "structural: true\nsql:\n  - create table t (a int)\n  - seed: insert into t values (1)\n"
	ok := "deps: [t]\nsql:\n  - ensure: select count(*) = 1 from t\n"
	h.mustApply(f("t", table, "c", ok))

	for name, sql := range map[string]string{
		"false":      "select false",
		"two rows":   "select true union all select true",
		"no rows":    "select true where false",
		"not bool":   "select 1",
		"two column": "select true, true",
	} {
		err := h.apply(f("t", table, "c", ok, "c2", "deps: [t]\nsql:\n  - ensure: "+sql+"\n"))
		if err == nil {
			t.Errorf("%s: the check should fail", name)
			continue
		}
		if !strings.Contains(err.Error(), "c2") && !strings.Contains(err.Error(), "check") {
			t.Errorf("%s: the error should name the check: %v", name, err)
		}
	}

	// A failing check names its mutation, position and first words.
	err := h.apply(f("t", table, "c", ok, "c2", "deps: [t]\nsql:\n  - seed: select 1\n  - ensure: select false\n"))
	if err == nil {
		t.Fatal("should fail")
	}
	attrs := fmt.Sprint(ErrorAttrs(err)...)
	for _, want := range []string{"c2", "statement_index2", "select false"} {
		if !strings.Contains(attrs, want) && !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q: %v %v", want, err, attrs)
		}
	}
}

func TestCheckRunsAgainWhenItsDependencyIsReapplied(t *testing.T) {
	h := newHarness(t)
	log := `structural: true
sql:
  - create table log (msg text)
  - create function log_it() returns boolean language sql as $$ insert into log values ('check'); select true $$
`
	z := "sql:\n  - seed: select 1\n"
	fn := func(deps string) string {
		return deps + "sql:\n  - up: create function f() returns int language sql as 'select 1'\n    down: drop function f()\n"
	}
	c := "deps: [f, log]\nsql:\n  - ensure: select log_it()\n"
	h.mustApply(f("log", log, "z", z, "f", fn("deps: [z]\n"), "c", c))
	h.want("select count(*) from log", "1")

	// z goes away: f is reapplied, then c, whose check runs again.
	h.mustApply(f("log", log, "f", fn(""), "c", c))
	h.wantRan("down:c(dep_reapplied:f) down:f(dep_reapplied:z) down:z up:f(dep_reapplied:z) up:c(dep_reapplied:f)")
	h.want("select count(*) from log", "2")
}

package mutations

import (
	"strings"
	"testing"
)

func TestTestPhaseRunsSeedsTwice(t *testing.T) {
	h := newHarness(t)
	table := "  - create table t (a int primary key)\n"
	// Not safe to run twice: the test phase finds out.
	err := h.apply(f("t", "structural: true\nsql:\n"+table+"  - seed: insert into t values (1)\n"))
	if err == nil || !strings.Contains(err.Error(), "duplicate key") {
		t.Errorf("want the duplicate key error, got %v", err)
	}
	h.mustApply(f("t", "structural: true\nsql:\n"+table+"  - seed: insert into t values (1) on conflict do nothing\n"))
	h.want("select count(*) from t", "1")
}

func TestTestPhaseFindsAWrongDown(t *testing.T) {
	h := newHarness(t)
	err := h.apply(f("t", "structural: true\nsql:\n  - up: create table t (a int)\n    down: select 1\n"))
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("want the test phase to find the lying down, got %v", err)
	}
	h.want("select to_regclass('t') is null", "true")
}

func TestTestPhaseLeavesTheDatabaseAsItWas(t *testing.T) {
	h := newHarness(t)
	v1 := "structural: true\nsql:\n  - create table t (a int)\n"
	h.mustApply(f("t", v1))
	h.query("insert into t values (1), (2)")
	h.mustApply(f("t", v1+"  - alter table t add column b int default 3\n"))
	h.want("select count(*), sum(b) from t", "2,6")
}

func TestTestPhaseOfAnOverrideCatchesALyingDown(t *testing.T) {
	h := newHarness(t)
	h.mustApply(f("r1", "revision: 1\nnamespace: x\nmutations:\n  t:\n    structural: true\n    sql:\n      - create table t (a int)\n"))
	h.query("create table lying (a int)") // adopted from elsewhere: the second pass needs it to exist
	err := h.apply(f("r2", `revision: 2
namespace: x
mutations:
  t:
    structural: true
    sql:
      - create table t (a int)
override:
  lying:
    structural: true
    sql:
      - up: create table lying (a int)
        down: select 1
`))
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("want the broken override to be caught, got %v", err)
	}
	h.want("select count(*) from __dmut__.mutations where namespace = 'x' and name = 'lying'", "0")
}

func TestTestPhaseIgnoresDeletedSiblingsOfAnOverride(t *testing.T) {
	h := newHarness(t)
	h.mustApply(f("r1", "revision: 1\nnamespace: x\nmutations:\n  s:\n    structural: true\n    sql:\n      - create schema s\n  s.t:\n    structural: true\n    sql:\n      - create table s.t (a int)\n"))
	// s.t is folded into the override of s.t2 and forgotten; its table stays, inside the schema.
	h.mustApply(f("r2", `revision: 2
namespace: x
mutations:
  s:
    structural: true
    sql:
      - create schema s
  s.t:
    structural: true
    sql:
      - create table s.t (a int)
override:
  s.t:
    structural: true
    sql:
      - create table s.t (a int)
delete: [s.t]
`))
	h.want("select count(*) from __dmut__.mutations where namespace = 'x'", "1")
}

package model

import (
	"reflect"
	"strings"
	"testing"
)

func TestExtractDocs(t *testing.T) {
	tests := []struct {
		name, sql string
		stripped  string
		want      []string
	}{
		{"no doc", "create table t (a int)", "create table t (a int)", nil},
		{"plain comment kept out", "-- hi\ncreate table t (a int)", "-- hi\ncreate table t (a int)", nil},
		{"table", "--? A table.\ncreate table t (a int)", "create table t (a int)",
			[]string{"COMMENT ON table t IS 'A table.'"}},
		{"stacked lines", "--? one\n--? two\n\ncreate table t (a int)", "\ncreate table t (a int)",
			[]string{"COMMENT ON table t IS 'one\ntwo'"}},
		{"block", "/*? A\n  table */\ncreate table t (a int)", "\ncreate table t (a int)",
			[]string{"COMMENT ON table t IS 'A\ntable'"}},
		{"quote", "--? it's\ncreate schema s", "create schema s",
			[]string{"COMMENT ON schema s IS 'it''s'"}},
		{"empty clears", "--?\ncreate schema s", "create schema s",
			[]string{"COMMENT ON schema s IS ''"}},
		{"columns", "--? tbl\ncreate table if not exists s.t (\n  --? the id\n  id int primary key,\n  name text not null default 'a,b',\n  /*? price */ price numeric(10, 2),\n  --? unique name\n  constraint t_name unique (name),\n  check (id > 0)\n)",
			"create table if not exists s.t (\n  id int primary key,\n  name text not null default 'a,b',\n price numeric(10, 2),\n  constraint t_name unique (name),\n  check (id > 0)\n)",
			[]string{
				"COMMENT ON table s.t IS 'tbl'",
				"COMMENT ON COLUMN s.t.id IS 'the id'",
				"COMMENT ON COLUMN s.t.price IS 'price'",
				"COMMENT ON CONSTRAINT t_name ON s.t IS 'unique name'",
			}},
		{"mid constraint", "create table t (\n a int --? positive\n constraint a_pos check (a > 0),\n b int\n)", "create table t (\n a int  constraint a_pos check (a > 0),\n b int\n)",
			[]string{"COMMENT ON CONSTRAINT a_pos ON t IS 'positive'"}},
		{"composite type", "create type pt as (\n --? x\n x int,\n --? y\n y int)", "create type pt as (\n x int,\n y int)",
			[]string{"COMMENT ON COLUMN pt.x IS 'x'", "COMMENT ON COLUMN pt.y IS 'y'"}},
		{"view columns", "create view v (\n --? a col\n a, b) as select 1, 2", "create view v (\n a, b) as select 1, 2",
			[]string{"COMMENT ON COLUMN v.a IS 'a col'"}},
		{"function", "--? adds\ncreate or replace function add(a int, b int) returns int language sql as $$ select a + b $$",
			"create or replace function add(a int, b int) returns int language sql as $$ select a + b $$",
			[]string{"COMMENT ON function add(a int, b int) IS 'adds'"}},
		{"index", "--? fast\ncreate unique index if not exists i on s.t (a)", "create unique index if not exists i on s.t (a)",
			[]string{"COMMENT ON index s.i IS 'fast'"}},
		{"trigger", "--? t\ncreate trigger tr before insert on t for each row execute function f()", "create trigger tr before insert on t for each row execute function f()",
			[]string{"COMMENT ON trigger tr on t IS 't'"}},
		{"add column", "--? new\nalter table s.t add column if not exists c int", "alter table s.t add column if not exists c int",
			[]string{"COMMENT ON COLUMN s.t.c IS 'new'"}},
		{"body keeps plain doc", "create function f() returns int language sql as $$ select 1 --? kept\n $$", "create function f() returns int language sql as $$ select 1 --? kept\n $$", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stripped, got, err := extractDocs(tt.sql)
			if err != nil {
				t.Fatal(err)
			}
			if stripped != tt.stripped {
				t.Errorf("stripped:\n%q\nwant:\n%q", stripped, tt.stripped)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got  %q\nwant %q", got, tt.want)
			}
		})
	}
}

func TestExtractDocsErrors(t *testing.T) {
	tests := map[string]string{
		"before a select":       "--? hi\nselect 1",
		"before add constraint": "--? hi\nalter table t add constraint c check (a > 0)",
		"unnamed constraint":    "create table t (\n a int,\n --? positive\n check (a > 0)\n)",
		"user mapping":          "--? hi\ncreate user mapping for bob server s",
		"enum value":            "create type e as enum (\n --? one\n 'one')",
		"function parameter":    "create function f(\n --? a\n a int) returns int language sql as $$ select 1 $$",
		"partition has no list": "create table p partition of t (\n --? a\n a) for values in (1)",
		"inside a check":        "create table t (\n a int check (\n --? x\n a > 0)\n)",
		"after the last column": "create table t (\n a int\n --? nothing\n)",
	}
	for name, sql := range tests {
		t.Run(name, func(t *testing.T) {
			if _, _, err := extractDocs(sql); err == nil {
				t.Fatal("expected an error")
			} else {
				t.Log(err)
			}
		})
	}
}

const docMutation = `structural: true
sql:
  - |
    --? A table.
    create table t (
      --? The id.
      id int
    )
`

func docFiles(extra string, sql string) map[string]string {
	return files("a", docMutation+extra, "b", "deps: [a]\nreapply_on: [dep_appended]\nsql:\n"+seeds(1))
}

func TestDocStatementsGoLast(t *testing.T) {
	sts := statementsOf(t, `
  - |
    --? A table.
    create table t (
      --? The id.
      id int
    )
  - alter table t add column b int
`)
	kinds := []Kind{KindReversible, KindReversible, KindDoc, KindDoc}
	if len(sts) != len(kinds) {
		t.Fatalf("got %d statements, want %d", len(sts), len(kinds))
	}
	for i, k := range kinds {
		if sts[i].Kind != k {
			t.Errorf("statement %d: kind %s, want %s", i+1, sts[i].Kind, k)
		}
	}
	if strings.Contains(sts[0].Up, "--?") || !strings.HasPrefix(strings.ToLower(sts[0].Down), "drop table") {
		t.Errorf("the create should be stripped of its docs and keep its down: %q / %q", sts[0].Up, sts[0].Down)
	}
	if sts[2].Up != "COMMENT ON table t IS 'A table.'" || sts[3].Up != "COMMENT ON COLUMN t.id IS 'The id.'" {
		t.Errorf("unexpected doc statements: %q, %q", sts[2].Up, sts[3].Up)
	}
}

func TestDocCommentsDoNotChangeTheCreateHash(t *testing.T) {
	a := statementsOf(t, "\n  - |\n    --? one\n    create table t (a int)\n")
	b := statementsOf(t, "\n  - |\n    --? two, longer\n    create table t (a int)\n")
	c := statementsOf(t, "\n  - create table t (a int)\n")
	if a[0].TextHash != b[0].TextHash || a[0].TextHash != c[0].TextHash {
		t.Error("a doc comment must not change the hash of the statement it documents")
	}
	if a[1].TextHash == b[1].TextHash {
		t.Error("the doc statement must change with its text")
	}
}

func TestPlanDocStatementsAreATail(t *testing.T) {
	old := docFiles("", "")
	edited := files("a", strings.Replace(docMutation, "The id.", "The identifier.", 1), "b", old["b.yml"])
	// Editing a comment reruns only the comments, and is fine in safe mode.
	p, err := planBetween(t, old, edited, planOpts{mode: ModeSafe})
	if err != nil {
		t.Fatalf("a comment edit should pass in safe mode: %v", err)
	}
	wantPlan(t, p, "down:a(tail 2) up:a(tail 2)")

	// A statement added at the end goes before the comments, and counts as an append.
	appended := docFiles("  - alter table t add column b int\n", "")
	p, err = planBetween(t, old, appended, planOpts{mode: ModeSafe})
	if err != nil {
		t.Fatalf("an append should pass in safe mode: %v", err)
	}
	wantPlan(t, p, "down:b(reapply dep_appended:a) down:a(tail 1) up:a(tail 1) up:b(reapply dep_appended:a)")

	// Editing the create is a real change.
	changed := files("a", strings.Replace(docMutation, "id int", "id bigint", 1), "b", old["b.yml"])
	if _, err := planBetween(t, old, changed, planOpts{mode: ModeSafe}); err == nil {
		t.Error("an edit of the create should be refused in safe mode")
	}

	// A comment edit alone does not wake the dependents that wait for appends.
	wantPlan(t, mustPlan(t, old, edited, planOpts{}), "down:a(tail 2) up:a(tail 2)")

	// Documenting a mutation that had no docs appends them.
	bare := files("a", "structural: true\nsql:\n  - create table t (a int)\n")
	documented := files("a", "structural: true\nsql:\n  - |\n    --? A table.\n    create table t (a int)\n")
	wantPlan(t, mustPlan(t, bare, documented, planOpts{mode: ModeSafe}), "up:a(append 1)")
}

// extractDocs is the statement inferDocs makes of sql, without its doc comments, and the
// COMMENT ON statements they ask for.
func extractDocs(sql string) (stripped string, comments []string, err error) {
	st, comments, err := inferDocs(sql, true)
	return st.Up, comments, err
}

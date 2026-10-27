package model

import (
	"reflect"
	"strings"
	"testing"
)

// Edge cases of extractDocs: they pin down the behaviour any grammar that reads doc comments
// has to keep.
func TestExtractDocsEdgeCases(t *testing.T) {
	tests := []struct {
		name, sql string
		want      []string
	}{
		{"upper case keywords", "--? x\nCREATE TABLE T (\n --? c\n A INT CONSTRAINT K CHECK (A > 0))",
			[]string{"COMMENT ON TABLE T IS 'x'", "COMMENT ON COLUMN T.A IS 'c'"}},
		{"quoted names", "--? q\ncreate table \"My Schema\".\"Odd Table\" (\n --? c\n \"Odd Col\" int,\n --? k\n constraint \"Odd Key\" unique (\"Odd Col\"))",
			[]string{"COMMENT ON table \"My Schema\".\"Odd Table\" IS 'q'", "COMMENT ON COLUMN \"My Schema\".\"Odd Table\".\"Odd Col\" IS 'c'", "COMMENT ON CONSTRAINT \"Odd Key\" ON \"My Schema\".\"Odd Table\" IS 'k'"}},
		{"temp unlogged global", "--? x\ncreate global temporary table t (a int)", []string{"COMMENT ON table t IS 'x'"}},
		{"unlogged", "--? x\ncreate unlogged table if not exists t (\n --? c\n a int)",
			[]string{"COMMENT ON table t IS 'x'", "COMMENT ON COLUMN t.a IS 'c'"}},
		{"column definitions with commas and parens", "create table t (\n --? a\n a numeric(10, 2) not null default round(1.5, 1) check (a in (1, 2)),\n --? b\n b int[] default '{1,2}'::int[] references o (id, x) on delete cascade,\n --? c\n c text collate \"C\" generated always as (a || ',' || b) stored,\n d int)",
			[]string{"COMMENT ON COLUMN t.a IS 'a'", "COMMENT ON COLUMN t.b IS 'b'", "COMMENT ON COLUMN t.c IS 'c'"}},
		{"two constraints in one column", "create table t (\n a int --? pos\n constraint a_pos check (a > 0) --? even\n constraint a_even check (a % 2 = 0))",
			[]string{"COMMENT ON CONSTRAINT a_pos ON t IS 'pos'", "COMMENT ON CONSTRAINT a_even ON t IS 'even'"}},
		{"table constraints of every kind", "create table t (\n a int,\n --? pk\n constraint t_pk primary key (a),\n --? fk\n constraint t_fk foreign key (a) references o (id),\n --? ex\n constraint t_ex exclude using gist (a with =),\n primary key (a))",
			[]string{"COMMENT ON CONSTRAINT t_pk ON t IS 'pk'", "COMMENT ON CONSTRAINT t_fk ON t IS 'fk'", "COMMENT ON CONSTRAINT t_ex ON t IS 'ex'"}},
		{"like and table options", "--? x\ncreate table t (\n like o including all,\n --? b\n b int\n) with (fillfactor = 70) tablespace ts",
			[]string{"COMMENT ON table t IS 'x'", "COMMENT ON COLUMN t.b IS 'b'"}},
		{"stacked mixed markers", "--? one\n/*? two */\n--? three\ncreate schema s", []string{"COMMENT ON schema s IS 'one\ntwo\nthree'"}},
		{"plain comment between", "--? one\n-- plain\n/* plain */\ncreate schema s", []string{"COMMENT ON schema s IS 'one'"}},
		{"crlf", "--? one\r\ncreate table t (\r\n --? c\r\n a int)\r\n", []string{"COMMENT ON table t IS 'one'", "COMMENT ON COLUMN t.a IS 'c'"}},
		{"empty block", "/*?*/ create schema s", []string{"COMMENT ON schema s IS ''"}},
		{"marker inside the text", "--? a --? b /*? c\ncreate schema s", []string{"COMMENT ON schema s IS 'a --? b /*? c'"}},
		{"dollar and backslash", "--? $$ \\n\ncreate schema s", []string{"COMMENT ON schema s IS '$$ \\n'"}},
		{"unicode text", "--? café ☕\ncreate table t (\n --? ünï\n c int)", []string{"COMMENT ON table t IS 'café ☕'", "COMMENT ON COLUMN t.c IS 'ünï'"}},
		{"trailing semicolon", "--? x\ncreate table t (\n --? c\n a int\n);", []string{"COMMENT ON table t IS 'x'", "COMMENT ON COLUMN t.a IS 'c'"}},
		{"or replace view with columns", "--? v\ncreate or replace view s.v (\n --? a\n a, b) with (security_barrier) as select 1, 2", []string{"COMMENT ON view s.v IS 'v'", "COMMENT ON COLUMN s.v.a IS 'a'"}},
		{"recursive view", "--? v\ncreate recursive view v (n) as select 1", []string{"COMMENT ON view v IS 'v'"}},
		{"view without a column list", "--? v\ncreate view v with (security_barrier) as select (1, 2)", []string{"COMMENT ON view v IS 'v'"}},
		{"materialized view if not exists", "--? m\ncreate materialized view if not exists m (\n --? a\n a) as select 1", []string{"COMMENT ON materialized view m IS 'm'", "COMMENT ON COLUMN m.a IS 'a'"}},
		{"foreign table", "--? f\ncreate foreign table f (\n --? a\n a int) server s", []string{"COMMENT ON foreign table f IS 'f'", "COMMENT ON COLUMN f.a IS 'a'"}},
		{"enum and range have no body", "--? e\ncreate type e as enum ('a', 'b')", []string{"COMMENT ON type e IS 'e'"}},
		{"range", "--? r\ncreate type r as range (subtype = int)", []string{"COMMENT ON type r IS 'r'"}},
		{"function defaults and out", "--? f\ncreate function s.f(in a int default 1, out b text, variadic c int[]) returns record language sql as $$ select 1, 'x' $$",
			[]string{"COMMENT ON function s.f(a int, c int[]) IS 'f'"}},
		{"aggregate", "--? a\ncreate aggregate s.a(int) (sfunc = int4pl, stype = int)", []string{"COMMENT ON aggregate s.a(int) IS 'a'"}},
		{"cast", "--? c\ncreate cast (int as money) with function f(int)", []string{"COMMENT ON cast(int as money) IS 'c'"}},
		{"event trigger", "--? e\ncreate event trigger e on ddl_command_end execute function f()", []string{"COMMENT ON event trigger e IS 'e'"}},
		{"extension", "--? e\ncreate extension if not exists pgcrypto", []string{"COMMENT ON extension pgcrypto IS 'e'"}},
		{"constraint trigger", "--? t\ncreate constraint trigger tr after insert or update of a, b on s.t deferrable for each row execute function f()", []string{"COMMENT ON trigger tr on s.t IS 't'"}},
		{"policy", "--? p\ncreate policy p on s.t using (true)", []string{"COMMENT ON policy p on s.t IS 'p'"}},
		{"rule", "--? r\ncreate or replace rule r as on delete to s.t do instead nothing", []string{"COMMENT ON rule r on s.t IS 'r'"}},
		{"operator class", "--? o\ncreate operator class c for type int using btree as operator 1 <", []string{"COMMENT ON operator class c using btree IS 'o'"}},
		{"text search", "--? t\ncreate text search dictionary d (template = simple)", []string{"COMMENT ON text search dictionary d IS 't'"}},
		{"role and tablespace", "--? r\ncreate role r login", []string{"COMMENT ON role r IS 'r'"}},
		{"add column variants", "--? c\nalter table s.t add column if not exists \"C\" int", []string{"COMMENT ON COLUMN s.t.\"C\" IS 'c'"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st, got, err := inferDocs(tt.sql, true)
			if err != nil {
				t.Fatal(err)
			}
			// Reading the doc comments must not change the down.
			if plain, err := Infer(st.Up); err != nil || plain.Down != st.Down {
				t.Errorf("down %q, without the doc comments %q (%v)", st.Down, plain.Down, err)
			}
			if !strings.EqualFold(strings.Join(got, "\n"), strings.Join(tt.want, "\n")) {
				t.Errorf("got  %q\nwant %q", got, tt.want)
			}
		})
	}
}

func TestExtractDocsEdgeErrors(t *testing.T) {
	tests := map[string]string{
		"unnamed primary key":        "create table t (\n a int,\n --? pk\n primary key (a))",
		"unnamed unique":             "create table t (\n a int,\n --? u\n unique (a))",
		"unnamed foreign key":        "create table t (\n a int,\n --? u\n foreign key (a) references o (id))",
		"like":                       "create table t (\n --? u\n like o)",
		"doc before references":      "create table t (\n a int --? u\n references o (id))",
		"doc before not null":        "create table t (\n a int --? u\n not null)",
		"two statements deep":        "--? x\ncreate table t (a int) partition by range (a)\n--? y",
		"typed table":                "create table t of ty (\n --? a\n a with options not null)",
		"partition":                  "create table p partition of t (\n --? a\n a) for values in (1)",
		"second add column":          "--? a\nalter table t add column a int,\n --? b\n add column b int",
		"alter other":                "--? a\nalter table t drop column a",
		"add without column keyword": "--? a\nalter table t add c int",
		"grant":                      "--? a\ngrant select on t to bob",
		"drop":                       "--? a\ndrop table t",
		"doc after create keyword":   "create --? a\ntable t (a int)",
		"doc between name and body":  "create table t --? a\n(a int)",
		"doc in function params":     "create function f(--? a\n a int) returns int language sql as 'select 1'",
		"doc in an index":            "create index i on t (--? a\n a)",
		"view body not a list":       "create view v as select (\n--? a\n 1)",
		"aggregate":                  "create aggregate a(\n--? a\n int) (sfunc = f, stype = int)",
	}
	for name, sql := range tests {
		t.Run(name, func(t *testing.T) {
			if _, _, err := extractDocs(sql); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

// A doc comment between statements of a plain string, or in a string that is not one
// statement, is the same load error as any other automatic statement.
func TestDocCommentsInTheLoader(t *testing.T) {
	sts := statementsOf(t, `
  - |
    --? one
    create table a (
      --? a.id
      id int
    )
  - auto: |
      --? two
      create table b (c int)
    settings: [dmut.env.X]
  - up: |
      --? kept as plain text, as up is not scanned
      create table c (c int)
    down: drop table c
  - seed: insert into a values (1)
`)
	var kinds []Kind
	var ups []string
	for _, s := range sts {
		kinds = append(kinds, s.Kind)
		ups = append(ups, s.Up)
	}
	wantKinds := []Kind{KindReversible, KindReversible, KindReversible, KindSeed, KindDoc, KindDoc, KindDoc}
	if !reflect.DeepEqual(kinds, wantKinds) {
		t.Fatalf("kinds %v, want %v", kinds, wantKinds)
	}
	want := []string{"COMMENT ON table a IS 'one'", "COMMENT ON COLUMN a.id IS 'a.id'", "COMMENT ON table b IS 'two'"}
	if !reflect.DeepEqual(ups[4:], want) {
		t.Errorf("docs %q, want %q", ups[4:], want)
	}
	if len(sts[1].Settings) != 1 || len(sts[2].Settings) != 0 {
		t.Errorf("settings must stay with the statement that wrote them: %+v %+v", sts[1].Settings, sts[2].Settings)
	}
	if !strings.Contains(sts[2].Up, "--? kept") {
		t.Errorf("up: is not scanned, got %q", sts[2].Up)
	}
}

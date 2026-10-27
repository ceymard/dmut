package model

import (
	"strings"
	"testing"
	"testing/fstest"
)

func load(t *testing.T, files map[string]string) (*Project, error) {
	t.Helper()
	fsys := fstest.MapFS{}
	for name, content := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(content)}
	}
	return LoadFS(fsys)
}

func mustLoad(t *testing.T, files map[string]string) *Project {
	t.Helper()
	p, err := load(t, files)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	return p
}

func wantError(t *testing.T, files map[string]string, parts ...string) {
	t.Helper()
	_, err := load(t, files)
	if err == nil {
		t.Fatalf("expected an error containing %q, got none", parts)
	}
	for _, part := range parts {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q should contain %q", err, part)
		}
	}
}

func TestPatterns(t *testing.T) {
	cases := []struct {
		pattern, name string
		want          bool
	}{
		{"api.*", "api.users", true},
		{"api.*", "api", false},
		{"api.*", "api.users.cols", false},
		{"api.**", "api.users.cols", true},
		{"api.**", "api", true},
		{"**", "a.b.c", true},
		{"*.users", "api.users", true},
		{"a.**.z", "a.z", true},
		{"a.**.z", "a.b.c.z", true},
		{"a.**.z", "a.b.c", false},
	}
	for _, c := range cases {
		if got := matchPattern(c.pattern, c.name); got != c.want {
			t.Errorf("matchPattern(%q, %q) = %v, want %v", c.pattern, c.name, got, c.want)
		}
	}
	for _, bad := range []string{"a*", "a.b*.c", "", "a..b", "a.%"} {
		if validPattern(bad) {
			t.Errorf("%q should not be a valid pattern", bad)
		}
	}
}

func TestDefaultMutationList(t *testing.T) {
	allowed := []string{
		"create or replace function f() returns int language sql as $$ select 1 $$",
		"create function f() returns int begin atomic select 1; end",
		"drop function f()",
		"alter function f() owner to bob",
		"create view v as select 1",
		"create or replace view v as select 1",
		"create trigger t before insert on x for each row execute function f()",
		"create constraint trigger t after insert on x for each row execute function f()",
		"create policy p on x using (true)",
		"create rule r as on insert to x do nothing",
		"create type pair as (a int, b int)",
		"drop type pair",
		"alter table x enable row level security",
		"alter table x force row level security, enable trigger t",
		"alter table x alter column c set default 1",
		"alter table x alter column c drop default",
		"grant select on x to bob",
		"revoke select on x from bob",
		"alter default privileges in schema s grant select on tables to bob",
		"comment on table x is 'hi'",
		"insert into x values (1)",
		"update x set a = 1",
		"delete from x",
		"truncate x",
		"copy x from stdin",
		"select 1",
		"with a as (select 1) select * from a",
		"do $$ begin perform 1; end $$",
		"call p()",
	}
	for _, sql := range allowed {
		if err := checkSQL(sql, false); err != nil {
			t.Errorf("%q should be allowed in a default mutation: %v", sql, err)
		}
	}
	refused := []string{
		"create table x (a int)",
		"alter table x add column a int",
		"alter table x enable row level security, add column a int",
		"drop table x",
		"create index i on x (a)",
		"create type color as enum ('a')",
		"alter type color add value 'b'",
		"create schema s",
		"create extension e",
		"create materialized view v as select 1",
		"drop materialized view v",
	}
	for _, sql := range refused {
		if err := checkSQL(sql, false); err == nil {
			t.Errorf("%q should be refused in a default mutation", sql)
		} else if !strings.Contains(err.Error(), "structural: true") {
			t.Errorf("%q: error should tell to set structural: %v", sql, err)
		}
		if err := checkSQL(sql, true); err != nil {
			t.Errorf("%q should be allowed in a structural mutation: %v", sql, err)
		}
	}
}

func TestTransactionControlRefusedEverywhere(t *testing.T) {
	for _, sql := range []string{
		"begin", "start transaction", "commit", "end", "rollback", "savepoint a",
		"release a", "prepare transaction 'x'", "select 1; commit",
	} {
		for _, structural := range []bool{false, true} {
			if err := checkSQL(sql, structural); err == nil {
				t.Errorf("%q should be refused (structural=%v)", sql, structural)
			}
		}
	}
	// Inside a dollar-quoted body and a begin atomic body, these words are not transaction control.
	for _, sql := range []string{
		"do $$ begin perform 1; end $$",
		"create function f() returns int begin atomic select case when true then 1 else 2 end; end",
		"prepare p as select 1",
	} {
		if err := checkSQL(sql, true); err != nil {
			t.Errorf("%q should pass: %v", sql, err)
		}
	}
}

func statementsOf(t *testing.T, yamlSQL string) []Statement {
	t.Helper()
	p := mustLoad(t, map[string]string{"m.yml": "structural: true\nsql:\n" + yamlSQL})
	return p.Namespaces[0].Revisions[1].Mutations[0].Statements
}

func TestStatementForms(t *testing.T) {
	sts := statementsOf(t, `
  - create table t (a int)
  - auto: create table u (a int)
  - up: create table v (a int)
    down: drop table v
  - seed: insert into t values (1)
  - check: select true
  - comment on table t is 'x'
  - alter type color add value if not exists 'a'
`)
	kinds := []Kind{KindReversible, KindReversible, KindReversible, KindSeed, KindCheck, KindSeed, KindSeed}
	if len(sts) != len(kinds) {
		t.Fatalf("got %d statements, want %d", len(sts), len(kinds))
	}
	for i, k := range kinds {
		if sts[i].Kind != k {
			t.Errorf("statement %d: kind %s, want %s", i+1, sts[i].Kind, k)
		}
	}
	if !strings.HasPrefix(strings.ToLower(sts[0].Down), "drop table") {
		t.Errorf("down should be inferred, got %q", sts[0].Down)
	}
	if sts[2].Down != "drop table v" || sts[3].Down != "" {
		t.Errorf("unexpected downs: %q, %q", sts[2].Down, sts[3].Down)
	}
}

func TestStatementErrors(t *testing.T) {
	cases := map[string]string{
		"two kinds":           "- {seed: select 1, check: select true}",
		"up without down":     "- {up: create table v (a int)}",
		"empty down":          "- {up: create table v (a int), down: ''}",
		"down with seed":      "- {seed: select 1, down: select 2}",
		"data with up":        "- {up: create table v (a int), down: drop table v, data: [[1]]}",
		"unknown key":         "- {seed: select 1, foo: 1}",
		"not inferable":       "- insert into t values (1)",
		"add value no ine":    "- alter type color add value 'a'",
		"check not select":   "- {check: delete from t}",
		"check two":          "- {check: 'select true; select true'}",
		"not a statement":     "- 12",
		"bad setting":         "- {seed: select 1, settings: [foo.bar]}",
		"short setting":       "- {seed: select 1, settings: [dmut.env]}",
		"hyphen in setting":   "- {seed: select 1, settings: [dmut.env.a-b]}",
		"digit first":         "- {seed: select 1, settings: [dmut.env.1a]}",
		"duplicate setting":   "- {seed: select 1, settings: [dmut.env.A, DMUT.env.a]}",
		"setting unknown key": "- {seed: select 1, settings: [{name: dmut.env.A, keep: true}]}",
		"data row length":     "- {seed: 'insert into t values ($1, $2)', data: [[1, 2], [3]]}",
		"data two statements": "- {seed: 'insert into t values ($1); select 1', data: [[1]]}",
		"data row not list":   "- {seed: 'insert into t values ($1)', data: [1]}",
		"transaction control": "- {seed: commit}",
		"several statements":  "- create table a (a int); create table b (a int)",
	}
	for name, sql := range cases {
		t.Run(name, func(t *testing.T) {
			wantError(t, map[string]string{"m.yml": "structural: true\nsql:\n  " + sql}, "m.yml", "statement 1")
		})
	}
}

func TestErrorNamesFileMutationPositionAndWords(t *testing.T) {
	wantError(t, map[string]string{"dir/api.users.yml": "sql:\n  - seed: select 1\n  - create table t (a int)\n"},
		"api.users", "statement 2", "create table t", "structural: true")
}

func TestSeedData(t *testing.T) {
	sts := statementsOf(t, `
  - seed: insert into roles (id, name, tags) values ($1, $2, $3) on conflict (id) do nothing
    data:
      - [1, admin, '["a", "b"]']
      - [2, guest, null]
      - [3, true, {a: 1}]
`)
	rows := sts[0].Data
	if len(rows) != 3 {
		t.Fatalf("got %d rows", len(rows))
	}
	if *rows[0][0] != "1" || *rows[0][1] != "admin" || *rows[0][2] != `["a", "b"]` {
		t.Errorf("row 1: %v", rows[0])
	}
	if rows[1][2] != nil {
		t.Errorf("a yaml null should be NULL, got %v", *rows[1][2])
	}
	if *rows[2][1] != "true" || *rows[2][2] != `{"a":1}` {
		t.Errorf("row 3: %s %s", *rows[2][1], *rows[2][2])
	}
}

func TestSeedDataAliasesAndHash(t *testing.T) {
	a := statementsOf(t, `
  - seed: insert into t values ($1, $2)
    data:
      - &row [1, x]
      - *row
`)[0]
	if len(a.Data) != 2 || *a.Data[1][1] != "x" {
		t.Fatalf("alias not expanded: %v", a.Data)
	}
	b := statementsOf(t, `
  - seed: insert into t values ($1, $2)
    data:
      - [1, x]
      - [1, x]
`)[0]
	if a.DataHash != b.DataHash {
		t.Error("equal data should hash equal")
	}
	c := statementsOf(t, `
  - seed: insert into t values ($1, $2)
    data:
      - [1, x]
      - [1, y]
`)[0]
	if a.DataHash == c.DataHash {
		t.Error("different data should hash differently")
	}
}

func TestSeedWithoutRowsAndHashIgnoresWhitespace(t *testing.T) {
	sts := statementsOf(t, `
  - {seed: 'insert into t values ($1)', data: []}
  - seed: select   1
  - seed: select 1
`)
	if len(sts[0].Data) != 0 {
		t.Error("no rows expected")
	}
	if sts[1].TextHash != sts[2].TextHash {
		t.Error("whitespace should not change the hash")
	}
}

func TestSettingsAndWarnings(t *testing.T) {
	p := mustLoad(t, map[string]string{"m.yml": `
structural: true
sql:
  - seed: select current_setting('dmut.env.A')
    settings:
      - dmut.env.A
      - {name: dmut.values.pw, tracked: false, saved: true}
      - {name: dmut.env.PARTITION, saved: true}
      - {name: dmut.env.OK, saved: true}
`})
	st := p.Namespaces[0].Revisions[1].Mutations[0].Statements[0]
	if len(st.Settings) != 4 {
		t.Fatalf("settings: %v", st.Settings)
	}
	if !st.Settings[0].Tracked || st.Settings[0].Saved {
		t.Errorf("defaults: %+v", st.Settings[0])
	}
	if st.Settings[1].Tracked || !st.Settings[1].Saved {
		t.Errorf("flags: %+v", st.Settings[1])
	}
	// Only the saved ones with a secret-looking name warn; "pw" is not on the list, "PARTITION" has "partition" only.
	if len(p.Warnings) != 0 {
		t.Errorf("unexpected warnings: %v", p.Warnings)
	}

	p = mustLoad(t, map[string]string{"m.yml": `
structural: true
sql:
  - seed: select 1
    settings:
      - {name: dmut.env.DB_PASSWORD, saved: true}
      - {name: dmut.env.DB_PASSWORD_2}
`})
	if len(p.Warnings) != 1 || p.Warnings[0].Position != 1 || p.Warnings[0].Mutation != "m" || !strings.Contains(p.Warnings[0].Message, "DB_PASSWORD") {
		t.Errorf("want one warning on the saved secret, got %v", p.Warnings)
	}
}

func TestMutationKeys(t *testing.T) {
	wantError(t, map[string]string{"m.yml": "sql: []\ncore: []\n"}, "m.yml", "unknown key core")
}

func TestMutationFileLayout(t *testing.T) {
	// A file named revision.yml is the mutation `revision`.
	p := mustLoad(t, map[string]string{"revision.yml": "sql:\n  - seed: select 1\n"})
	if _, ok := p.Namespaces[0].Revisions[1].Mutation("revision"); !ok {
		t.Error("revision.yml should define the mutation revision")
	}

	// The directory has no meaning, the namespace key does.
	p = mustLoad(t, map[string]string{
		"a/b/api.users.yml": "namespace: x\nsql:\n  - seed: select 1\n",
		"api.yml":           "namespace: x\n",
	})
	rev := p.Namespaces[0].Revisions[1]
	if p.Namespaces[0].Name != "x" || len(rev.Mutations) != 2 {
		t.Fatalf("unexpected load: %+v", p.Namespaces[0])
	}
	users, _ := rev.Mutation("api.users")
	if len(users.Parents) != 1 || users.Parents[0].Name != "api" {
		t.Errorf("dotted name should depend on its parent: %v", users.Parents)
	}

	// `revision` anywhere but first, and `mutations`, are not mutation keys.
	wantError(t, map[string]string{"m.yml": "sql: []\nrevision: 1\n"}, "unknown key revision")
	wantError(t, map[string]string{"m.yml": "mutations: {}\n"}, "unknown key mutations")

	wantError(t, map[string]string{"bad name.yml": "sql: []\n"}, "not a valid mutation name")
	wantError(t, map[string]string{"a..b.yml": "sql: []\n"}, "not a valid mutation name")
}

func TestRevisionDocument(t *testing.T) {
	p := mustLoad(t, map[string]string{"any.yml": `
revision: 1
namespace: x
mode: force
delete: [old]
override:
  a:
    sql: [{seed: select 1}]
mutations:
  a:
    sql: [{seed: select 1}]
  mode:
    sql: [{seed: select 2}]
  b:
    deps: [a]
`})
	rev := p.Namespaces[0].Revisions[1]
	if rev.Mode != ModeForce || len(rev.Delete) != 1 || len(rev.Override) != 1 || len(rev.Mutations) != 3 {
		t.Fatalf("unexpected revision: %+v", rev)
	}
	if _, ok := rev.Mutation("mode"); !ok {
		t.Error("a mutation named mode should load under mutations:")
	}

	wantError(t, map[string]string{"r.yml": "revision: 1\nmode: fast\n"}, "unknown mode")
	wantError(t, map[string]string{"r.yml": "revision: 1\nfoo: 1\n"}, "unknown key foo")
	wantError(t, map[string]string{"r.yml": "revision: 0\n"}, "positive integer")
	wantError(t, map[string]string{"r.yml": "mutations: {}\nrevision: 1\n"}, "unknown key mutations")
}

func TestRevisionNumbering(t *testing.T) {
	files := map[string]string{
		"r1.yml": "revision: 1\nnamespace: x\nmutations: {a: {sql: [{seed: select 1}]}}\n",
		"r2.yml": "revision: 2\nnamespace: x\nmutations: {a: {sql: [{seed: select 2}]}}\n",
		"a.yml":  "namespace: x\nsql: [{seed: select 3}]\n",
	}
	p := mustLoad(t, files)
	ns := p.Namespaces[0]
	if ns.Min != 1 || ns.Max != 3 || len(ns.Revisions) != 3 {
		t.Fatalf("revisions: %d..%d (%d)", ns.Min, ns.Max, len(ns.Revisions))
	}
	if m, _ := ns.Revisions[3].Mutation("a"); m == nil || m.Statements[0].Up != "select 3" {
		t.Error("mutation files should sit at the revision after the highest")
	}

	wantError(t, map[string]string{
		"r1.yml": "revision: 1\nnamespace: x\n",
		"r3.yml": "revision: 3\nnamespace: x\n",
	}, "gap")

	// With no revision document the unnamed revision is 1.
	p = mustLoad(t, map[string]string{"a.yml": "sql: [{seed: select 1}]\n"})
	if p.Namespaces[0].Max != 1 {
		t.Error("unnamed revision alone should be 1")
	}
}

func TestDuplicateDefinitions(t *testing.T) {
	wantError(t, map[string]string{
		"a/m.yml": "sql: [{seed: select 1}]\n",
		"b/m.yml": "sql: [{seed: select 2}]\n",
	}, "defined twice", "m.yml")
	wantError(t, map[string]string{
		"m.yml": "revision: 1\nmutations: {a: {}}\n---\nrevision: 1\nmutations: {a: {}}\n",
	}, "defined twice")
	// The same name in two namespaces is fine.
	mustLoad(t, map[string]string{
		"a/m.yml": "namespace: one\n",
		"b/m.yml": "namespace: two\n",
	})
}

func TestDependencies(t *testing.T) {
	p := mustLoad(t, map[string]string{
		"a.yml":     "{}\n",
		"a.b.yml":   "{}\n",
		"a.b.c.yml": "deps: [a.b, 'x.*']\n",
		"x.1.yml":   "{}\n",
		"x.2.yml":   "{}\n",
		"x.2.z.yml": "{}\n",
		"y.yml":     "deps: ['**', {name: nothing.*, reapply_on: []}]\n",
	})
	rev := p.Namespaces[0].Revisions[1]
	c, _ := rev.Mutation("a.b.c")
	var names []string
	for _, d := range c.Dependencies() {
		names = append(names, d.Name)
	}
	if got := strings.Join(names, ","); got != "a,a.b,x.1,x.2" {
		t.Errorf("a.b.c depends on %s", got)
	}
	y, _ := rev.Mutation("y")
	if len(y.Matched[0]) != 6 {
		t.Errorf("** should match every other mutation, got %d", len(y.Matched[0]))
	}
	for _, m := range y.Matched[0] {
		if m == y {
			t.Error("a mutation never matches its own pattern")
		}
	}
	if len(y.Matched[1]) != 0 || y.Deps[1].ReapplyOn == nil || len(y.Deps[1].ReapplyOn) != 0 {
		t.Errorf("a pattern matching nothing is allowed, and an empty reapply_on stays empty: %+v", y.Deps[1])
	}

	order, err := rev.UpOrder()
	if err != nil {
		t.Fatal(err)
	}
	pos := map[string]int{}
	for i, m := range order {
		pos[m.Name] = i
	}
	for _, m := range order {
		for _, d := range m.Dependencies() {
			if pos[d.Name] >= pos[m.Name] {
				t.Errorf("%s must come after its dependency %s", m.Name, d.Name)
			}
		}
	}
}

func TestDependencyErrors(t *testing.T) {
	wantError(t, map[string]string{"a.yml": "deps: [missing]\n"}, "missing", "does not exist")
	wantError(t, map[string]string{"a.yml": "deps: [a]\n"}, "cycle", "a -> a")
	wantError(t, map[string]string{
		"a.yml": "deps: [b]\n",
		"b.yml": "deps: [c]\n",
		"c.yml": "deps: [a]\n",
	}, "cycle", "a -> b -> c -> a")
	// A parent that waits for its own child.
	wantError(t, map[string]string{"a.yml": "deps: [a.b]\n", "a.b.yml": "{}\n"}, "cycle")
	wantError(t, map[string]string{"a.yml": "deps: ['a*']\n"}, "not a valid dependency")
	wantError(t, map[string]string{"a.yml": "reapply_on: [sometimes]\n"}, "unknown reapply trigger")
	wantError(t, map[string]string{"a.yml": "deps: [{name: b, foo: 1}]\n", "b.yml": "{}\n"}, "unknown dependency key")
}

func TestDefaultReapplyOn(t *testing.T) {
	p := mustLoad(t, map[string]string{
		"d.yml": "{}\n",
		"s.yml": "structural: true\n",
		"o.yml": "reapply_on: [deps_grow]\n",
		"e.yml": "deps: [{name: d, reapply_on: [dep_appended]}]\n",
	})
	rev := p.Namespaces[0].Revisions[1]
	get := func(name string) *Mutation { m, _ := rev.Mutation(name); return m }
	if got := get("d").DefaultReapplyOn(); len(got) != 1 || got[0] != DepReapplied {
		t.Errorf("default mutation: %v", got)
	}
	if got := get("s").DefaultReapplyOn(); got == nil || len(got) != 0 {
		t.Errorf("structural mutation: %v", got)
	}
	if got := get("o").DefaultReapplyOn(); len(got) != 1 || got[0] != DepsGrow {
		t.Errorf("mutation reapply_on: %v", got)
	}
	e := get("e")
	if got := e.EffectiveReapplyOn(e.Deps[0]); len(got) != 1 || got[0] != DepAppended {
		t.Errorf("entry reapply_on: %v", got)
	}
}

func TestLoadPathsFileAndDirectory(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		if err := writeFile(dir, name, content); err != nil {
			t.Fatal(err)
		}
	}
	write("a.yml", "sql: [{seed: select 1}]\n")
	write("sub/b.yaml", "deps: [a]\n")
	write("notes.txt", "ignored\n")
	p, err := LoadPaths(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Namespaces[0].Revisions[1].Mutations) != 2 {
		t.Error("both yaml extensions should load, and other files be ignored")
	}
	p, err = LoadPaths(dir + "/a.yml")
	if err != nil || len(p.Namespaces[0].Revisions[1].Mutations) != 1 {
		t.Errorf("a single file path should load alone: %v", err)
	}
	if _, err := LoadPaths(dir + "/missing"); err == nil {
		t.Error("a missing path should fail")
	}
}

func TestEmptyDocumentWarns(t *testing.T) {
	p := mustLoad(t, map[string]string{"a.yml": "# nothing\n", "b.yml": "sql: [{seed: select 1}]\n"})
	if len(p.Warnings) != 1 || p.Warnings[0].File != "a.yml" {
		t.Errorf("want one warning on a.yml, got %v", p.Warnings)
	}
}

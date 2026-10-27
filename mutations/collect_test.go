package mutations

import (
	"bytes"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/ceymard/dmut/v3/mutations/model"
)

func loadText(t *testing.T, files map[string]string) *model.Project {
	t.Helper()
	fsys := fstest.MapFS{}
	for name, content := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(content)}
	}
	p, err := model.LoadFS(fsys)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

const everything = `revision: 3
namespace: shop
mode: force
delete: [old]
override:
  items:
    structural: true
    sql:
      - create table items (id int, name text, born date)
mutations:
  items:
    structural: true
    sql:
      - create table items (id int)
      - alter table items add column name text
  items.seed:
    deps: [items, {name: 'other.*', reapply_on: [dep_appended, deps_grow]}]
    reapply_on: [dep_reapplied]
    sql:
      - comment on table items is 'hello'
      - up: create function f() returns int language sql as 'select 1'
        down: drop function f()
      - auto: create view v as select * from items
        settings: [dmut.env.A, {name: dmut.values.b, tracked: false, saved: true}]
      - seed: insert into items values ($1, $2, $3)
        data:
          - [1, "007", null]
          - [2, "a: b", "-1"]
          - [3, true, '{"k": [1, 2]}']
        settings: [dmut.env.A]
      - check: select count(*) > 0 from items
  other.a: {}
`

func TestCollectRoundTrips(t *testing.T) {
	p := loadText(t, map[string]string{"in.yml": everything})
	var first bytes.Buffer
	if err := Collect(p, &first); err != nil {
		t.Fatal(err)
	}
	p2 := loadText(t, map[string]string{"out.yml": first.String()})
	var second bytes.Buffer
	if err := Collect(p2, &second); err != nil {
		t.Fatal(err)
	}
	if first.String() != second.String() {
		t.Errorf("collecting twice should give the same file:\n%s\n----\n%s", first.String(), second.String())
	}

	rev := p2.Namespaces[0].Revisions[3]
	if rev.Mode != model.ModeForce || len(rev.Delete) != 1 || len(rev.Override) != 1 {
		t.Errorf("revision keys lost: %+v", rev)
	}
	seed, _ := rev.Mutation("items.seed")
	st := seed.Statements
	if st[0].Kind != model.KindSeed || st[1].Down != "drop function f()" || st[2].Kind != model.KindReversible {
		t.Errorf("statements changed: %+v", st)
	}
	if !st[2].Settings[1].Saved || st[2].Settings[1].Tracked {
		t.Errorf("settings lost: %+v", st[2].Settings)
	}
	rows := st[3].Data
	if len(rows) != 3 || *rows[0][1] != "007" || rows[0][2] != nil || *rows[1][1] != "a: b" || *rows[1][2] != "-1" || *rows[2][2] != `{"k": [1, 2]}` {
		t.Errorf("data changed: %v", rows)
	}
	if st[4].Kind != model.KindCheck {
		t.Errorf("check lost: %+v", st[4])
	}
	if seed.ReapplyOn == nil || len(seed.Deps) != 2 || len(seed.Deps[1].ReapplyOn) != 2 {
		t.Errorf("dependencies lost: %+v", seed.Deps)
	}
}

func TestCollectLeavesDmutOut(t *testing.T) {
	p, err := LoadProject()
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	if err := Collect(p, &b); err != nil || b.Len() != 0 {
		t.Errorf("the embedded namespace should not be written, got %q %v", b.String(), err)
	}
}

func TestExplode(t *testing.T) {
	p := loadText(t, map[string]string{"in.yml": everything})
	files := map[string]string{}
	if err := Explode(p, func(name, contents string) error { files[name] = contents; return nil }); err != nil {
		t.Fatal(err)
	}
	// The effective definition: items is the override, old is gone, and no revision key.
	if len(files) != 3 || !strings.Contains(files["items.yml"], "born date") || strings.Contains(files["items.yml"], "revision") || !strings.HasPrefix(files["items.yml"], "namespace: shop") {
		t.Fatalf("unexpected files: %v", files)
	}
	back := loadText(t, files)
	if len(back.Namespaces) != 1 || len(back.Namespaces[0].Revisions[1].Mutations) != 3 {
		t.Error("exploded files should load back as one revision of three mutations")
	}
}

func TestRevisionShellFromLocalAndDatabase(t *testing.T) {
	p := loadText(t, map[string]string{"in.yml": everything})
	shells, warnings := CollectRevisionFromLocal(p, 4)
	if len(shells) != 1 || len(warnings) != 0 || len(shells[0].Mutations) != 3 {
		t.Fatalf("shells %v warnings %v", shells, warnings)
	}
	if _, w := CollectRevisionFromLocal(p, 9); len(w) != 1 {
		t.Errorf("a revision that is not the next one should warn, got %v", w)
	}
	var b bytes.Buffer
	if err := CollectRevisionShell(shells, &b); err != nil {
		t.Fatal(err)
	}
	loaded := loadText(t, map[string]string{"shell.yml": b.String()})
	if loaded.Namespaces[0].Max != 4 || len(loaded.Namespaces[0].Revisions[4].Mutations) != 3 {
		t.Errorf("the shell should load back: %s", b.String())
	}
}

func TestRevisionShellFromTheDatabase(t *testing.T) {
	h := newHarness(t)
	h.mustApply(f("a", `structural: true
sql:
  - create table a (id int)
  - seed: insert into a values ($1) on conflict do nothing
    data: [[1]]
    settings: [{name: dmut.values.k, saved: true}]
`), func(o *Options) { o.Values = map[string]string{"k": "v"} })

	r, err := NewRunner(t.Context(), h.uri, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(t.Context())
	shells, warnings, err := r.CollectRevisionFromDatabase(t.Context(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(shells) != 1 || shells[0].Namespace != "" && len(shells[0].Mutations) != 1 {
		t.Fatalf("unexpected shells: %+v", shells)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "data that is not stored") {
		t.Errorf("want a warning about the lost data, got %v", warnings)
	}
}

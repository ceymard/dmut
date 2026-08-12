package mutations

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// write drops the given files in a fresh directory and returns its path.
// Keys are file names, values are their yaml content.
func write(t *testing.T, files map[string]string) string {
	t.Helper()

	dir := t.TempDir()
	for name, content := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// load reads the yaml in a fresh directory built from files.
func load(t *testing.T, files map[string]string) (*MutationNamespace, error) {
	t.Helper()
	return LoadYamlMutations(write(t, files))
}

// mustLoad fails the test if the mutations do not load.
func mustLoad(t *testing.T, files map[string]string) *MutationNamespace {
	t.Helper()

	ns, err := load(t, files)
	if err != nil {
		t.Fatalf("expected the mutations to load, got: %v", err)
	}
	return ns
}

// refuses checks that loading fails with a message containing want.
func refuses(t *testing.T, want string, files map[string]string) {
	t.Helper()

	_, err := load(t, files)
	if err == nil {
		t.Fatalf("expected an error containing %q, got none", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Errorf("expected an error containing %q, got: %v", want, err)
	}
}

// set returns the only mutation set of a namespace.
func set(t *testing.T, ns *MutationNamespace, namespace string) *MutationSet {
	t.Helper()

	seq, ok := ns.Map.Get(namespace)
	if !ok {
		t.Fatalf("namespace %q was not loaded", namespace)
	}
	if len(seq.Revisions) != 1 {
		t.Fatalf("expected a single revision in namespace %q, got %d", namespace, len(seq.Revisions))
	}
	for _, s := range seq.Revisions {
		return s
	}
	return nil
}

func mutation(t *testing.T, s *MutationSet, name string) *Mutation {
	t.Helper()

	mut, ok := s.GetMutation(name)
	if !ok {
		t.Fatalf("mutation %s was not loaded", name)
	}
	return mut
}

// hasParent reports whether name is among the mutation's sql or meta parents.
func hasParent(mut *Mutation, name string, meta bool) bool {
	parents := mut.SqlParents
	if meta {
		parents = mut.MetaParents
	}
	for _, parent := range parents.Values() {
		if parent.Name == name {
			return true
		}
	}
	return false
}

// //////////////////////////////////////////////////////////////
// What a mutation file may contain

func TestUnknownKeysAreRefused(t *testing.T) {
	// `roles` was a key in dmut before 1.0 and is the one people will try first.
	refuses(t, "unknown key 'roles'", map[string]string{
		"a.yml": "x:\n  roles: [admin]\n  sql:\n    - create schema x;\n",
	})
	refuses(t, "unknown key 'up'", map[string]string{
		"a.yml": "x:\n  up:\n    - create schema x;\n",
	})
}

func TestStatementsMayBeStringsOrPairs(t *testing.T) {
	s := set(t, mustLoad(t, map[string]string{
		"a.yml": `x:
  sql:
    - create schema x;
    - up: create table x.t (id int);
      down: drop table x.t;
`,
	}), "")

	mut := mutation(t, s, "x")
	if len(mut.Sql) != 2 {
		t.Fatalf("expected 2 statements, got %d", len(mut.Sql))
	}
	if mut.Sql[0].Down != "DROP schema x;" {
		t.Errorf("expected an inferred down, got %q", mut.Sql[0].Down)
	}
	if mut.Sql[1].Down != "drop table x.t;" {
		t.Errorf("expected the explicit down to be kept, got %q", mut.Sql[1].Down)
	}
}

func TestStatementsThatCannotBeUndoneAreRefused(t *testing.T) {
	refuses(t, "can't generate undo statement", map[string]string{
		"a.yml": "x:\n  sql:\n    - update t set a = 1;\n",
	})
}

// //////////////////////////////////////////////////////////////
// Dependencies

func TestDottedNamesDependOnTheirAncestors(t *testing.T) {
	s := set(t, mustLoad(t, map[string]string{
		"a.yml": `a:
  sql:
    - create schema a;
a.b:
  sql:
    - create table a.t (id int);
a.b.c:
  sql:
    - create index i on a.t (id);
a.unrelated:
  sql:
    - create table a.u (id int);
`,
	}), "")

	child := mutation(t, s, "a.b.c")
	for _, ancestor := range []string{"a", "a.b"} {
		if !hasParent(child, ancestor, false) {
			t.Errorf("a.b.c should depend on %s for sql", ancestor)
		}
		if !hasParent(child, ancestor, true) {
			t.Errorf("a.b.c should depend on %s for meta", ancestor)
		}
	}
	if hasParent(child, "a.unrelated", false) {
		t.Error("a.b.c must not depend on its sibling a.unrelated")
	}
}

func TestChildrenBecomeDottedMutations(t *testing.T) {
	s := set(t, mustLoad(t, map[string]string{
		"a.yml": `parent:
  sql:
    - create table t (id int);
  children:
    child:
      sql:
        - alter table t add column name text;
`,
	}), "")

	child := mutation(t, s, "parent.child")
	if !hasParent(child, "parent", false) {
		t.Error("a child mutation should depend on its parent")
	}
}

func TestMissingDependencyIsRefused(t *testing.T) {
	refuses(t, "asks for dependency", map[string]string{
		"a.yml": "x:\n  needs: [nope]\n  sql:\n    - create schema x;\n",
	})
	refuses(t, "asks for meta dependency", map[string]string{
		"a.yml": "x:\n  meta_needs: [nope]\n  sql:\n    - create schema x;\n",
	})
}

func TestDependenciesCannotCrossNamespaces(t *testing.T) {
	refuses(t, "asks for dependency", map[string]string{
		"a.yml": "__namespace: one\nx:\n  sql:\n    - create schema x;\n",
		"b.yml": "__namespace: two\ny:\n  needs: [x]\n  sql:\n    - create schema y;\n",
	})
}

func TestDependenciesCannotCrossRevisions(t *testing.T) {
	refuses(t, "asks for dependency", map[string]string{
		"a.yml": "__revision: 1\nx:\n  sql:\n    - create schema x;\n",
		"b.yml": "__revision: 2\ny:\n  needs: [x]\n  sql:\n    - create schema y;\n",
	})
}

func TestCyclesAreRefused(t *testing.T) {
	refuses(t, "cycle", map[string]string{
		"a.yml": `x:
  needs: [z]
  sql:
    - create schema x;
y:
  needs: [x]
  sql:
    - create schema y;
z:
  needs: [y]
  sql:
    - create schema z;
`,
	})
}

// A mutation defined twice would otherwise be silently dropped, and the version
// you edited might never run.
func TestDuplicateNamesAcrossFilesAreRefused(t *testing.T) {
	refuses(t, "duplicate mutation name", map[string]string{
		"a.yml": "x:\n  sql:\n    - create schema x;\n",
		"b.yml": "x:\n  sql:\n    - create schema other;\n",
	})
}

func TestDuplicateNamesInOneFileAreRefused(t *testing.T) {
	refuses(t, "already defined", map[string]string{
		"a.yml": "x:\n  sql:\n    - create schema one;\nx:\n  sql:\n    - create schema two;\n",
	})
}

// The same name in two namespaces is fine : they are separate silos.
func TestSameNameInTwoNamespacesIsFine(t *testing.T) {
	ns := mustLoad(t, map[string]string{
		"a.yml": "__namespace: one\nx:\n  sql:\n    - create schema x1;\n",
		"b.yml": "__namespace: two\nx:\n  sql:\n    - create schema x2;\n",
	})

	for _, name := range []string{"one", "two"} {
		mutation(t, set(t, ns, name), "x")
	}
}

// //////////////////////////////////////////////////////////////
// Namespaces and revisions

func TestFilesAreGroupedByNamespace(t *testing.T) {
	ns := mustLoad(t, map[string]string{
		"a.yml":     "__namespace: one\nx:\n  sql:\n    - create schema x;\n",
		"sub/b.yml": "__namespace: one\ny:\n  sql:\n    - create schema y;\n",
		"c.yml":     "__namespace: two\nz:\n  sql:\n    - create schema z;\n",
	})

	one := set(t, ns, "one")
	mutation(t, one, "x")
	mutation(t, one, "y")

	mutation(t, set(t, ns, "two"), "z")
}

func TestUnrevisionedFilesAreTheNextRevision(t *testing.T) {
	for _, tt := range []struct {
		name  string
		files map[string]string
		want  int
	}{
		{
			name:  "no revision at all is revision 1",
			files: map[string]string{"a.yml": "x:\n  sql:\n    - create schema x;\n"},
			want:  1,
		},
		{
			name: "unrevisioned files come after the highest revision",
			files: map[string]string{
				"r1.yml": "__revision: 1\nx:\n  sql:\n    - create schema x;\n",
				"r2.yml": "__revision: 2\nx:\n  sql:\n    - create schema x;\n",
				"a.yml":  "y:\n  sql:\n    - create schema y;\n",
			},
			want: 3,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ns := mustLoad(t, tt.files)
			seq, ok := ns.Map.Get("")
			if !ok {
				t.Fatal("the default namespace was not loaded")
			}
			if seq.MaxRevision != tt.want {
				t.Errorf("expected the highest revision to be %d, got %d", tt.want, seq.MaxRevision)
			}
		})
	}
}

func TestRevisionGapsAreRefused(t *testing.T) {
	refuses(t, "not continuous", map[string]string{
		"r1.yml": "__revision: 1\nx:\n  sql:\n    - create schema x;\n",
		"r3.yml": "__revision: 3\nx:\n  sql:\n    - create schema x;\n",
	})
}

func TestRevisionsAreSeparateSets(t *testing.T) {
	ns := mustLoad(t, map[string]string{
		"r1.yml": "__revision: 1\nx:\n  sql:\n    - create schema x;\n",
		"r2.yml": "__revision: 2\nx:\n  sql:\n    - create schema x;\n  new_sql:\n    - create schema x2;\n",
	})

	seq, _ := ns.Map.Get("")
	if len(seq.Revisions) != 2 {
		t.Fatalf("expected 2 revisions, got %d", len(seq.Revisions))
	}
	if !seq.Revisions[2].HasOverrides {
		t.Error("revision 2 declares new_sql and should be flagged as having overrides")
	}
	if seq.Revisions[1].HasOverrides {
		t.Error("revision 1 declares no new_sql and should not be flagged")
	}
}

// An empty new_sql retires a mutation and must not count as an override : there
// is no cleaned-up definition for the next revision to inherit.
func TestEmptyNewSqlIsNotAnOverride(t *testing.T) {
	ns := mustLoad(t, map[string]string{
		"r1.yml": "__revision: 1\nx:\n  sql:\n    - create schema x;\n  new_sql: []\n",
	})

	seq, _ := ns.Map.Get("")
	if seq.Revisions[1].HasOverrides {
		t.Error("an empty new_sql should not flag the set as having overrides")
	}
}

// AsNewMutationSet is what the next revision compares itself against.
func TestNewValuesReplaceTheOldOnes(t *testing.T) {
	s := set(t, mustLoad(t, map[string]string{
		"r1.yml": `__revision: 1
base:
  sql:
    - create schema base;
x:
  needs: [base]
  sql:
    - create schema x;
  new_needs: []
  new_sql:
    - create schema x2;
`,
	}), "")

	replaced := mutation(t, s.AsNewMutationSet(), "x")
	if len(replaced.Sql) != 1 || replaced.Sql[0].Up != "create schema x2;" {
		t.Errorf("expected new_sql to replace sql, got %+v", replaced.Sql)
	}
	if len(replaced.NewSql) != 0 {
		t.Error("the new set should not carry new_sql any more")
	}
}

// //////////////////////////////////////////////////////////////
// What makes a mutation different

func TestCommentsAndWhitespaceDoNotChangeTheHash(t *testing.T) {
	a := mutation(t, set(t, mustLoad(t, map[string]string{
		"a.yml": "x:\n  sql:\n    - create schema x;\n",
	}), ""), "x")

	b := mutation(t, set(t, mustLoad(t, map[string]string{
		"a.yml": "x:\n  sql:\n    - |\n      -- a comment\n      create    schema\n      x; /* and another */\n",
	}), ""), "x")

	if a.SqlHash() != b.SqlHash() {
		t.Error("comments and whitespace should not change the hash of a mutation")
	}
}

func TestChangingOnlyTheDownChangesTheHash(t *testing.T) {
	a := mutation(t, set(t, mustLoad(t, map[string]string{
		"a.yml": "x:\n  sql:\n    - up: create schema x;\n      down: drop schema x;\n",
	}), ""), "x")

	b := mutation(t, set(t, mustLoad(t, map[string]string{
		"a.yml": "x:\n  sql:\n    - up: create schema x;\n      down: drop schema x cascade;\n",
	}), ""), "x")

	if a.SqlHash() == b.SqlHash() {
		t.Error("the down of a statement is part of what makes a mutation different")
	}
}

func TestSqlAndMetaAreHashedApart(t *testing.T) {
	files := func(meta string) map[string]string {
		return map[string]string{
			"a.yml": "x:\n  sql:\n    - create table t (id int);\n  meta:\n    - " + meta + "\n",
		}
	}

	a := mutation(t, set(t, mustLoad(t, files("grant select on table t to public;")), ""), "x")
	b := mutation(t, set(t, mustLoad(t, files("grant insert on table t to public;")), ""), "x")

	if a.MetaHash() == b.MetaHash() {
		t.Error("changing meta should change the meta hash")
	}
	if a.SqlHash() != b.SqlHash() {
		t.Error("changing meta must not change the sql hash")
	}
}

func TestRenamingChangesTheHash(t *testing.T) {
	a := mutation(t, set(t, mustLoad(t, map[string]string{
		"a.yml": "x:\n  sql:\n    - create schema s;\n",
	}), ""), "x")

	b := mutation(t, set(t, mustLoad(t, map[string]string{
		"a.yml": "y:\n  sql:\n    - create schema s;\n",
	}), ""), "y")

	if a.SqlHash() == b.SqlHash() {
		t.Error("the name of a mutation is part of its hash")
	}
}

// //////////////////////////////////////////////////////////////
// Reading files

func TestOnlyYamlFilesAreRead(t *testing.T) {
	ns := mustLoad(t, map[string]string{
		"a.yml":      "x:\n  sql:\n    - create schema x;\n",
		"b.yaml":     "y:\n  sql:\n    - create schema y;\n",
		"notes.txt":  "this is not yaml at all",
		"README.md":  "# neither is this",
		"sub/c.yaml": "z:\n  sql:\n    - create schema z;\n",
	})

	s := set(t, ns, "")
	for _, name := range []string{"x", "y", "z"} {
		mutation(t, s, name)
	}
	if s.Size() != 3 {
		t.Errorf("expected 3 mutations, got %d", s.Size())
	}
}

// dmut needs its own bookkeeping mutations wherever it is pointed.
func TestDmutOwnMutationsAreAlwaysLoaded(t *testing.T) {
	ns := mustLoad(t, map[string]string{
		"a.yml": "x:\n  sql:\n    - create schema x;\n",
	})

	if _, ok := ns.Map.Get(DmutNamespace); !ok {
		t.Errorf("the %s namespace should always be loaded", DmutNamespace)
	}
}

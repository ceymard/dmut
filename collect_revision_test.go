package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/ceymard/dmut/v2/mutations"
)

// widgetsR1/widgetsR2 are a small two-revision fixture applied straight to a fresh
// database (so only r2, the highest revision, actually runs) - r2 is override-shaped:
// its child mutation runs for real via mutations:, then gets folded away by delete:,
// and override: records the consolidated single-table definition instead.
const widgetsR1 = `__namespace: widgets
__revision: 1

widgets:
  core:
    - create table widgets (id serial primary key, name text not null);
`

const widgetsR2 = `__namespace: widgets
__revision: 2

delete: [widgets.color]

mutations:
  widgets:
    core:
      - create table widgets (id serial primary key, name text not null);
    children:
      color:
        core:
          - alter table widgets add column color text;

override:
  widgets:
    core:
      - create table widgets (id serial primary key, name text not null, color text);
`

// A minimal single-mutation fixture for a given namespace, used to populate
// __dmut__.mutations with something to enumerate/source from.
func namespacedFixture(namespace string) string {
	return "__namespace: " + namespace + `

thing:
  core:
    - create schema ` + namespace + `;
`
}

func newPgRunner(t *testing.T, uri string) *mutations.PgRunner {
	t.Helper()

	runner, err := mutations.NewPgRunner(uri, false)
	if err != nil {
		t.Fatalf("could not connect: %v", err)
	}
	t.Cleanup(func() { runner.Close() })
	return runner
}

func TestGetDBNamespacesListsDistinctNamespaces(t *testing.T) {
	uri := testdb(t)
	mustApply(t, uri, mutationDir(t, map[string]string{
		"one.yml": namespacedFixture("one"),
		"two.yml": namespacedFixture("two"),
	}))

	runner := newPgRunner(t, uri)
	namespaces, err := runner.GetDBNamespaces()
	if err != nil {
		t.Fatalf("GetDBNamespaces: %v", err)
	}

	want := map[string]bool{"one": true, "two": true}
	if len(namespaces) != len(want) {
		t.Fatalf("expected %d namespaces, got %v", len(want), namespaces)
	}
	for _, ns := range namespaces {
		if !want[ns] {
			t.Errorf("unexpected namespace %q in %v", ns, namespaces)
		}
		delete(want, ns)
	}
	if len(want) != 0 {
		t.Errorf("missing namespaces: %v", want)
	}
}

// __dmut__ is dmut's own bookkeeping namespace and must never show up as
// something a user would collect a revision shell for.
func TestGetDBNamespacesExcludesDmutNamespace(t *testing.T) {
	uri := testdb(t)
	mustApply(t, uri, mutationDir(t, map[string]string{"one.yml": namespacedFixture("one")}))

	runner := newPgRunner(t, uri)
	namespaces, err := runner.GetDBNamespaces()
	if err != nil {
		t.Fatalf("GetDBNamespaces: %v", err)
	}
	for _, ns := range namespaces {
		if ns == mutations.DmutNamespace {
			t.Errorf("GetDBNamespaces must not include %s, got %v", mutations.DmutNamespace, namespaces)
		}
	}
}

// A fresh, never-applied-to database has no __dmut__ schema at all yet - this
// must report no namespaces, not error.
func TestGetDBNamespacesOnFreshDatabase(t *testing.T) {
	uri := testdb(t)

	runner := newPgRunner(t, uri)
	namespaces, err := runner.GetDBNamespaces()
	if err != nil {
		t.Fatalf("GetDBNamespaces: %v", err)
	}
	if len(namespaces) != 0 {
		t.Errorf("expected no namespaces on a fresh database, got %v", namespaces)
	}
}

// The database-source workflow: what's recorded (already the post-override,
// effective shape - the retired child is gone, the override's consolidated core is
// what's there) is exactly what CollectRevisionFromDatabase scaffolds into
// mutations:, with no warning when given the expected next revision number.
func TestCollectRevisionFromDatabaseSourcesRecordedMutations(t *testing.T) {
	uri := testdb(t)
	mustApply(t, uri, mutationDir(t, map[string]string{
		"r1.yml": widgetsR1,
		"r2.yml": widgetsR2,
	}))

	runner := newPgRunner(t, uri)
	shells, warnings, err := mutations.CollectRevisionFromDatabase(runner, 3)
	if err != nil {
		t.Fatalf("CollectRevisionFromDatabase: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("expected no warnings for the correct next revision, got %v", warnings)
	}

	var buf bytes.Buffer
	if err := mutations.CollectRevisionShell(shells, &buf); err != nil {
		t.Fatalf("CollectRevisionShell: %v", err)
	}
	got := buf.String()

	if !strings.Contains(got, "__namespace: widgets") || !strings.Contains(got, "__revision: 3") {
		t.Errorf("expected a shell for widgets at revision 3:\n%s", got)
	}
	if !strings.Contains(got, "name text not null, color text") {
		t.Errorf("expected the recorded (post-override) consolidated core:\n%s", got)
	}
	if strings.Contains(got, "color:") || strings.Contains(got, "widgets.color") {
		t.Errorf("the retired child must not reappear, only the folded-in column:\n%s", got)
	}
}

// A revision number that doesn't match the database's own recorded revision + 1
// produces a warning naming both numbers, but still returns a usable shell.
func TestCollectRevisionFromDatabaseWarnsOnWrongNumber(t *testing.T) {
	uri := testdb(t)
	mustApply(t, uri, mutationDir(t, map[string]string{
		"r1.yml": widgetsR1,
		"r2.yml": widgetsR2,
	}))

	runner := newPgRunner(t, uri)
	shells, warnings, err := mutations.CollectRevisionFromDatabase(runner, 7)
	if err != nil {
		t.Fatalf("CollectRevisionFromDatabase: %v", err)
	}
	if len(warnings) != 1 {
		t.Fatalf("expected exactly one warning, got %v", warnings)
	}
	if !strings.Contains(warnings[0], "7 given, expected 3") {
		t.Errorf("warning does not mention the given/expected numbers: %q", warnings[0])
	}
	if len(shells) != 1 {
		t.Fatalf("expected the shell to still be produced despite the warning, got %d", len(shells))
	}
}

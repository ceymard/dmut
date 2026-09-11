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

// gadgetsR1/gadgetsR2 are a plain (non-override-shaped) two-revision history - r2
// adds a real child column, tracked separately, with no squashing yet - used to
// exercise collect -r's actual scaffold-then-hand-fill workflow end to end, unlike
// widgetsR1/R2 above which are already squashed.
const gadgetsR1 = `__namespace: gadgets
__revision: 1

gadget:
  core:
    - create table gadget (id serial primary key, name text not null);
`

const gadgetsR2 = `__namespace: gadgets
__revision: 2

gadget:
  core:
    - create table gadget (id serial primary key, name text not null);

gadget.weight:
  needs: [gadget]
  core:
    - alter table gadget add column weight int;
`

// The full collect -r loop: scaffold a revision shell from real accumulated history,
// hand-fill its override: the way a user actually would, apply it to the database
// that produced the history, and confirm the result is structurally indistinguishable
// from a fresh database landing on the equivalent hand-written revision directly -
// proving the shell collect -r hands you is actually usable, not just well-formed.
func TestCollectRevisionShellRoundTripsIntoARealApply(t *testing.T) {
	uriA := testdb(t)
	dir := mutationDir(t, map[string]string{
		"r1.yml": gadgetsR1,
		"r2.yml": gadgetsR2,
	})

	mustApply(t, uriA, dir+"/r1.yml")
	mustApply(t, uriA, dir)
	exec(t, uriA, "insert into gadget (name, weight) values ('sprocket', 5);")

	// Scaffold revision 3 from the accumulated local history (r1+r2) - MaxRevision
	// is 2, so 3 is the correct next number, and the shell's mutations: should carry
	// both gadget and its weight child forward unchanged, per collect -r's own local
	// workflow (CollectRevisionFromLocal).
	muts, err := mutations.LoadYamlMutations(dir)
	if err != nil {
		t.Fatalf("error loading %s: %v", dir, err)
	}
	shells, warnings, err := mutations.CollectRevisionFromLocal(muts, 3)
	if err != nil {
		t.Fatalf("CollectRevisionFromLocal: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings scaffolding the correct next revision, got %v", warnings)
	}

	var buf bytes.Buffer
	if err := mutations.CollectRevisionShell(shells, &buf); err != nil {
		t.Fatalf("CollectRevisionShell: %v", err)
	}
	scaffold := buf.String()
	for _, want := range []string{"__revision: 3", "gadget", "gadget.weight", "mode: default", "delete: []"} {
		if !strings.Contains(scaffold, want) {
			t.Fatalf("expected the scaffold to contain %q:\n%s", want, scaffold)
		}
	}

	// Hand-fill override: and delete: the way a user reading that scaffold would -
	// mutations: restated unchanged (exactly what the scaffold already carries),
	// weight folded into gadget's own CREATE TABLE and retired from tracking.
	r3 := `__namespace: gadgets
__revision: 3

delete: [gadget.weight]

override:
  gadget:
    core:
      - create table gadget (id serial primary key, name text not null, weight int);

mutations:
  gadget:
    core:
      - create table gadget (id serial primary key, name text not null);

  gadget.weight:
    needs: [gadget]
    core:
      - alter table gadget add column weight int;
`
	mustApply(t, uriA, mutationDir(t, map[string]string{"r3.yml": r3}))

	if got := queryInt(t, uriA, "select weight from gadget where name = 'sprocket'"); got != 5 {
		t.Errorf("expected the pre-existing row to survive untouched (no real change happened), got weight=%d", got)
	}
	if got := savedMutations(t, uriA, "gadgets"); got != 1 {
		t.Errorf("expected only gadget left tracked after folding weight away, got %d", got)
	}

	// r3 itself still runs the old two-step history for real on a fresh database too
	// (mutations: restates it, and mutations: always executes for real regardless of
	// override: - only what's *recorded* differs) - the one-step payoff shows up one
	// revision later, in a plain, minimal file that inherits r3's override, exactly
	// like examples/revisions' r3.yml inherits r2's. r4 is that file: what you'd
	// actually keep in the repository once r3 has done its job.
	uriB := testdb(t)
	mustApply(t, uriB, mutationDir(t, map[string]string{"r3.yml": r3}))

	if !columnExists(t, uriB, "gadget", "weight") {
		t.Error("expected the fresh database to end up with weight either way")
	}
	if got := savedMutations(t, uriB, "gadgets"); got != 1 {
		t.Errorf("expected only gadget tracked on the fresh database too, got %d", got)
	}

	r4 := `__namespace: gadgets
__revision: 4

gadget:
  core:
    - create table gadget (id serial primary key, name text not null, weight int);
`
	uriC := testdb(t)
	mustApply(t, uriC, mutationDir(t, map[string]string{"r4.yml": r4}))

	if !columnExists(t, uriC, "gadget", "weight") {
		t.Error("expected a fresh database on r4 to have weight too")
	}
	if got := savedMutations(t, uriC, "gadgets"); got != 1 {
		t.Errorf("expected only gadget tracked - the one-step build, no separate weight mutation at all, got %d", got)
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

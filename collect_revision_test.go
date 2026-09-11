package main

import (
	"testing"

	"github.com/ceymard/dmut/v2/mutations"
)

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

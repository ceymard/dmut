package mutations

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// collect loads the given paths and returns the collected yaml.
func collect(t *testing.T, paths ...string) string {
	t.Helper()

	muts, err := LoadYamlMutations(paths...)
	if err != nil {
		t.Fatalf("error loading %v: %v", paths, err)
	}

	var buf bytes.Buffer
	if err := Collect(muts, &buf); err != nil {
		t.Fatalf("error collecting %v: %v", paths, err)
	}
	return buf.String()
}

// Collecting an already collected file must be a no-op, otherwise the output is
// not something anyone can keep in a repository.
func TestCollectRoundTrip(t *testing.T) {
	for _, path := range []string{"test/test.yml", "../test/revision"} {
		t.Run(path, func(t *testing.T) {
			first := collect(t, path)

			out := filepath.Join(t.TempDir(), "collected.yml")
			if err := os.WriteFile(out, []byte(first), 0644); err != nil {
				t.Fatal(err)
			}

			second := collect(t, out)
			if first != second {
				t.Errorf("collect is not stable\n--- first ---\n%s\n--- second ---\n%s", first, second)
			}
		})
	}
}

// hashes returns every mutation's sql and meta hash, keyed by namespace and name.
func hashes(t *testing.T, paths ...string) map[string][2]string {
	t.Helper()

	muts, err := LoadYamlMutations(paths...)
	if err != nil {
		t.Fatalf("error loading %v: %v", paths, err)
	}

	res := make(map[string][2]string)
	for _, ns := range muts.Keys() {
		if ns == DmutNamespace {
			continue
		}
		seq, _ := muts.Map.Get(ns)
		for revision, set := range seq.Revisions {
			for mut := range set.AllMutations() {
				key := ns + "/" + strconv.Itoa(revision) + "/" + mut.Name
				res[key] = [2]string{mut.SqlHash(), mut.MetaHash()}
			}
		}
	}
	return res
}

// Collecting must not change a single hash, otherwise replacing your mutation
// files with a collected one would de-apply and re-apply the whole database.
func TestCollectPreservesHashes(t *testing.T) {
	for _, path := range []string{"test/test.yml", "../test/revision"} {
		t.Run(path, func(t *testing.T) {
			before := hashes(t, path)

			out := filepath.Join(t.TempDir(), "collected.yml")
			if err := os.WriteFile(out, []byte(collect(t, path)), 0644); err != nil {
				t.Fatal(err)
			}
			after := hashes(t, out)

			if len(before) != len(after) {
				t.Fatalf("expected %d mutations after collecting, got %d", len(before), len(after))
			}
			for key, want := range before {
				got, ok := after[key]
				if !ok {
					t.Errorf("%s disappeared from the collected file", key)
					continue
				}
				if got[0] != want[0] {
					t.Errorf("%s: sql hash changed", key)
				}
				if got[1] != want[1] {
					t.Errorf("%s: meta hash changed", key)
				}
			}
		})
	}
}

// dmut's own mutations ship embedded in the binary and would collide with
// themselves if they were written out.
func TestCollectSkipsDmutNamespace(t *testing.T) {
	if got := collect(t, "test/test.yml"); strings.Contains(got, DmutNamespace) {
		t.Errorf("collected output must not contain the %s namespace:\n%s", DmutNamespace, got)
	}
}

// A retired mutation is one with an explicitly empty new_sql : dropping the
// distinction between "empty" and "absent" would silently un-retire it.
func TestCollectKeepsEmptyNewSql(t *testing.T) {
	got := collect(t, "../test/revision")
	if !strings.Contains(got, "new_sql: []") {
		t.Errorf("expected an empty new_sql to survive collection:\n%s", got)
	}
}

// Dependencies dmut derives from dotted names are not written back out.
func TestCollectOmitsImplicitNeeds(t *testing.T) {
	got := collect(t, "../test/revision")
	if strings.Contains(got, "needs: [test]") {
		t.Errorf("implicit parent dependency should not be collected:\n%s", got)
	}
}

// Several documents in one file are several sets, each with its own namespace
// and revision. This is what collect relies on to write everything to one file.
func TestReadMultipleDocumentsPerFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "multi.yml")
	if err := os.WriteFile(path, []byte(`
__namespace: one
a:
  sql:
    - create schema a;
---
__namespace: two
b:
  sql:
    - create schema b;
`), 0644); err != nil {
		t.Fatal(err)
	}

	muts, err := LoadYamlMutations(path)
	if err != nil {
		t.Fatalf("error loading multi-document file: %v", err)
	}

	for _, ns := range []string{"one", "two"} {
		seq, ok := muts.Map.Get(ns)
		if !ok {
			t.Fatalf("namespace %s was not read", ns)
		}
		if len(seq.Revisions) != 1 {
			t.Errorf("namespace %s: expected 1 revision, got %d", ns, len(seq.Revisions))
		}
	}
}

package mutations

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// collectRevisionFromLocal loads paths and builds revision shells at revision,
// returning the emitted yaml and any warnings.
func collectRevisionFromLocal(t *testing.T, revision int, paths ...string) (string, []string) {
	t.Helper()

	muts, err := LoadYamlMutations(paths...)
	if err != nil {
		t.Fatalf("error loading %v: %v", paths, err)
	}

	shells, warnings, err := CollectRevisionFromLocal(muts, revision)
	if err != nil {
		t.Fatalf("CollectRevisionFromLocal: %v", err)
	}

	var buf bytes.Buffer
	if err := CollectRevisionShell(shells, &buf); err != nil {
		t.Fatalf("CollectRevisionShell: %v", err)
	}
	return buf.String(), warnings
}

// The correct next revision number produces no warnings, and a scaffold carrying
// the full mode/delete/override/mutations template with mutations: sourced from the
// namespace's current (highest-revision) effective definition.
func TestCollectRevisionFromLocalBuildsScaffold(t *testing.T) {
	got, warnings := collectRevisionFromLocal(t, 4, "../test/revision")
	if len(warnings) != 0 {
		t.Errorf("expected no warnings for the correct next revision, got %v", warnings)
	}

	for _, want := range []string{
		"__revision: 4",
		"mode: default",
		"delete: []",
		"override: {}",
		"mutations:",
		// revision 3's consolidated core (the namespace's current effective state).
		"age integer not null default 0",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("expected output to contain %q:\n%s", want, got)
		}
	}
}

// A revision number that doesn't look like n+1 produces a warning naming both the
// given and expected numbers, but must not abort - the shell still gets written.
func TestCollectRevisionFromLocalWarnsOnWrongNumber(t *testing.T) {
	got, warnings := collectRevisionFromLocal(t, 9, "../test/revision")
	if len(warnings) != 1 {
		t.Fatalf("expected exactly one warning, got %v", warnings)
	}
	if !strings.Contains(warnings[0], "9 given, expected 4") {
		t.Errorf("warning does not mention the given/expected numbers: %q", warnings[0])
	}
	if !strings.Contains(got, "__revision: 9") {
		t.Errorf("expected the shell to still be written at the given revision:\n%s", got)
	}
}

// When the namespace's latest local revision is itself override-shaped, the shell's
// mutations: must be sourced from its *effective* (WithOverrides) state, not its raw
// mutations: block - otherwise collecting a shell for revision 3 right after writing
// revision 2's override would scaffold the wrong (pre-override) definition.
func TestCollectRevisionFromLocalUsesEffectiveStateWhenLatestIsOverrideShaped(t *testing.T) {
	got, warnings := collectRevisionFromLocal(t, 3,
		"../test/revision/test-r1.yml", "../test/revision/test-r2.yml")
	if len(warnings) != 0 {
		t.Errorf("expected no warnings, got %v", warnings)
	}

	// r2's override.test.core is the consolidated 3-column table - the effective
	// state - not r2's own mutations.test.core (2 columns) or the still-tracked
	// add_column_age child (which r2's delete: retires).
	if !strings.Contains(got, "age integer not null default 0") {
		t.Errorf("expected the override's consolidated core, not the pre-override one:\n%s", got)
	}
	if strings.Contains(got, "add_column_age") {
		t.Errorf("add_column_age should have been folded away (retired by r2's delete:), not restated:\n%s", got)
	}
}

// The emitted shell must itself be valid, loadable dmut yaml - a scaffold nobody can
// load back in to hand-edit isn't useful.
func TestCollectRevisionFromLocalOutputIsLoadable(t *testing.T) {
	got, _ := collectRevisionFromLocal(t, 4, "../test/revision")

	out := filepath.Join(t.TempDir(), "shell.yml")
	if err := os.WriteFile(out, []byte(got), 0644); err != nil {
		t.Fatal(err)
	}

	if _, err := LoadYamlMutations(out); err != nil {
		t.Fatalf("emitted shell does not load back: %v\n%s", err, got)
	}
}

// __dmut__ is dmut's own bookkeeping namespace and must never be offered as
// something to scaffold a revision shell for.
func TestCollectRevisionFromLocalSkipsDmutNamespace(t *testing.T) {
	got, _ := collectRevisionFromLocal(t, 4, "../test/revision")
	if strings.Contains(got, DmutNamespace) {
		t.Errorf("shell output must not contain the %s namespace:\n%s", DmutNamespace, got)
	}
}

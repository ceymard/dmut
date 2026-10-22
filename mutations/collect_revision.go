package mutations

import (
	"context"
	"fmt"
	"io"
)

// newRevisionShell builds an override-shaped MutationSet at revision, populated with
// a fresh, dependency-resolved copy of every mutation in effective - a scaffold ready
// to write out via CollectRevisionShell. Mode/Delete/OverrideMutations are always set
// (Mode to "default", Delete to an empty-but-non-nil slice, OverrideMutations to an
// empty-but-non-nil set) so the emitted document always carries the full template from
// specs/changes.md ("Changes to collect"), ready for the user to hand-fill.
func newRevisionShell(namespace string, revision int, effective *MutationSet) (*MutationSet, error) {
	shell := NewMutationSet(namespace, revision, "")
	shell.OverrideShaped = true
	shell.Mode = "default"
	shell.Delete = []string{}
	shell.OverrideMutations = NewMutationSet(namespace, revision, "")

	for mut := range effective.AllMutations() {
		// A fresh copy, not the same pointer: effective may be a still-live set (the
		// namespace's own latest revision) whose graph ResolveDependencies below must
		// not be allowed to mutate out from under it - see WithOverrides' identical
		// concern and the Phase 3 pointer-aliasing bug it documents.
		cp := *mut
		if err := shell.AddMutation(&cp); err != nil {
			return nil, err
		}
	}
	if err := shell.ResolveDependencies(); err != nil {
		return nil, err
	}
	return shell, nil
}

// CollectRevisionFromLocal builds one revision shell per namespace found in
// namespaces (skipping __dmut__), sourced from each namespace's current effective
// definition - its latest revision's mutations, or that revision's WithOverrides()
// result when it is itself override-shaped. This is the primary `collect -r`
// workflow: turn already-accumulated local files into a revision shell ready for
// override: to be hand-written into. warnings reports, per namespace, when revision
// doesn't look like the expected next one (n+1) - never an error, since the user may
// have a deliberate reason to skip numbers.
func CollectRevisionFromLocal(namespaces *MutationNamespace, revision int) (shells []*MutationSet, warnings []string, err error) {
	for _, ns := range namespaces.Keys() {
		if ns == DmutNamespace {
			continue
		}
		seq, ok := namespaces.Map.Get(ns)
		if !ok {
			continue
		}

		if revision != seq.MaxRevision+1 {
			warnings = append(warnings, fmt.Sprintf("namespace %q: revision %d given, expected %d (highest local revision is %d)", ns, revision, seq.MaxRevision+1, seq.MaxRevision))
		}

		latest := seq.Revisions[seq.MaxRevision]
		effective := latest
		if latest.OverrideMutations != nil {
			effective = latest.WithOverrides()
		}

		shell, shell_err := newRevisionShell(ns, revision, effective)
		if shell_err != nil {
			return nil, nil, shell_err
		}
		shells = append(shells, shell)
	}
	return shells, warnings, nil
}

// CollectRevisionFromDatabase builds one revision shell per namespace recorded in a
// live database, sourced from what's actually recorded there (GetDBMutationsFromDb
// already returns the post-override, effective shape - no WithOverrides needed). This
// is the secondary `collect -r` workflow, for an orphaned database with no local
// files. warnings mirrors CollectRevisionFromLocal's "does revision look like n+1"
// check, against the database's own recorded revision for each namespace.
func CollectRevisionFromDatabase(ctx context.Context, runner Executor, revision int) (shells []*MutationSet, warnings []string, err error) {
	namespaces, err := runner.GetDBNamespaces(ctx)
	if err != nil {
		return nil, nil, err
	}

	for _, ns := range namespaces {
		distant, err := runner.GetDBMutationsFromDb(ctx, ns)
		if err != nil {
			return nil, nil, err
		}

		if revision != distant.Revision+1 {
			warnings = append(warnings, fmt.Sprintf("namespace %q: revision %d given, expected %d (database is recorded at revision %d)", ns, revision, distant.Revision+1, distant.Revision))
		}

		shell, err := newRevisionShell(ns, revision, distant)
		if err != nil {
			return nil, nil, err
		}
		shells = append(shells, shell)
	}
	return shells, warnings, nil
}

// CollectRevisionShell writes one yaml document per shell, in the full scaffold shape
// from specs/changes.md ("Changes to collect") - mode, delete, override and mutations
// always all present, regardless of what's actually populated in them, since the
// point of a shell is to hand the user a template to fill in by hand.
func CollectRevisionShell(shells []*MutationSet, w io.Writer) error {
	out := &yamlWriter{w: w}

	for i, shell := range shells {
		if i > 0 {
			out.line("---")
		}

		if shell.Namespace != "" {
			out.field(0, "__namespace", shell.Namespace)
		}
		out.fieldInt(0, "__revision", shell.Revision)

		out.blank()
		out.field(0, "mode", shell.Mode)

		out.blank()
		out.stringListForce(0, "delete", shell.Delete)

		out.blank()
		out.line("override: {} # fill in the consolidated definition this revision promises here")

		out.blank()
		out.line("mutations:")
		emitMutationBodies(out, 1, shell)
	}

	return out.err
}

package mutations

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/ceymard/dmut/v3/mutations/model"
)

// Collect writes every mutation of every namespace back out as yaml, one revision document
// per namespace and revision, sorted by name so the result is stable and diffable. The
// output is read back by dmut: statements whose down can be inferred are written as plain
// strings, the rest as mappings. dmut's own namespace is left out.
func Collect(p *model.Project, w io.Writer) error {
	out := &yamlWriter{w: w}
	first := true
	for _, ns := range p.Namespaces {
		if ns.Name == DmutNamespace {
			continue
		}
		numbers := make([]int, 0, len(ns.Revisions))
		for n := range ns.Revisions {
			numbers = append(numbers, n)
		}
		sort.Ints(numbers)
		for _, n := range numbers {
			rev := ns.Revisions[n]
			if !first {
				out.line("---")
			}
			first = false
			out.fieldInt(0, "revision", n)
			if ns.Name != "" {
				out.field(0, "namespace", ns.Name)
			}
			if rev.Mode != "" {
				out.field(0, "mode", string(rev.Mode))
			}
			out.stringList(0, "delete", rev.Delete)
			if len(rev.Override) > 0 {
				out.blank()
				out.line("override:")
				emitMutations(out, 1, rev.Override)
			}
			out.blank()
			out.line("mutations:")
			emitMutations(out, 1, rev.Mutations)
		}
	}
	return out.err
}

// Shell is a revision to fill in by hand: the effective mutations of a namespace, ready to be
// consolidated with override:.
type Shell struct {
	Namespace string
	Revision  int
	Mutations []*model.Mutation
}

// CollectRevisionFromLocal builds one shell per namespace of p, sourced from the effective
// definition of its highest revision. A warning is returned for a namespace when revision
// is not the next one.
func CollectRevisionFromLocal(p *model.Project, revision int) (shells []*Shell, warnings []string) {
	for _, ns := range p.Namespaces {
		if ns.Name == DmutNamespace {
			continue
		}
		if revision != ns.Max+1 {
			warnings = append(warnings, fmt.Sprintf("namespace %q: revision %d given, expected %d (highest local revision is %d)", ns.Name, revision, ns.Max+1, ns.Max))
		}
		shells = append(shells, &Shell{Namespace: ns.Name, Revision: revision, Mutations: ns.Revisions[ns.Max].Effective()})
	}
	return shells, warnings
}

// CollectRevisionFromDatabase builds one shell per namespace recorded in the database, from
// what is recorded there. Data of seeds and settings that were not stored cannot be recovered; a
// warning says where.
func (r *Runner) CollectRevisionFromDatabase(ctx context.Context, revision int) (shells []*Shell, warnings []string, err error) {
	names, err := r.Namespaces(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, name := range names {
		state, current, err := r.LoadState(ctx, name)
		if err != nil {
			return nil, nil, err
		}
		if revision != current+1 {
			warnings = append(warnings, fmt.Sprintf("namespace %q: revision %d given, expected %d (database is recorded at revision %d)", name, revision, current+1, current))
		}
		recs := make([]string, 0, len(state))
		for n := range state {
			recs = append(recs, n)
		}
		sort.Strings(recs)
		shell := &Shell{Namespace: name, Revision: revision}
		for _, n := range recs {
			m, lost := model.FromRecorded(state[n], name)
			for _, pos := range lost {
				warnings = append(warnings, fmt.Sprintf("namespace %q: statement %d of %s has data that is not stored: it is left out", name, pos, n))
			}
			shell.Mutations = append(shell.Mutations, m)
		}
		shells = append(shells, shell)
	}
	return shells, warnings, nil
}

// CollectRevisionShell writes one revision document per shell, with mode, delete and
// override present for the author to fill in.
func CollectRevisionShell(shells []*Shell, w io.Writer) error {
	out := &yamlWriter{w: w}
	for i, shell := range shells {
		if i > 0 {
			out.line("---")
		}
		out.fieldInt(0, "revision", shell.Revision)
		if shell.Namespace != "" {
			out.field(0, "namespace", shell.Namespace)
		}
		out.blank()
		out.field(0, "mode", string(model.ModeNormal))
		out.blank()
		out.stringListForce(0, "delete", nil)
		out.blank()
		out.line("override: {} # fill in the consolidated definition this revision promises here")
		out.blank()
		out.line("mutations:")
		emitMutations(out, 1, shell.Mutations)
	}
	return out.err
}

// Explode writes one file for each effective mutation of the highest revision of each namespace
// to dir, named after the mutation, with its namespace and no revision.
func Explode(p *model.Project, write func(name string, contents string) error) error {
	for _, ns := range p.Namespaces {
		if ns.Name == DmutNamespace {
			continue
		}
		for _, m := range ns.Revisions[ns.Max].Effective() {
			var b strings.Builder
			out := &yamlWriter{w: &b}
			if ns.Name != "" {
				out.field(0, "namespace", ns.Name)
			}
			emitMutationBody(out, 0, m)
			if out.err != nil {
				return out.err
			}
			if err := write(m.Name+".yml", b.String()); err != nil {
				return err
			}
		}
	}
	return nil
}

// emitMutations writes mutations, sorted by name, at the given depth: 1 when nested under
// mutations: or override:.
func emitMutations(out *yamlWriter, depth int, muts []*model.Mutation) {
	sorted := append([]*model.Mutation(nil), muts...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	pad := strings.Repeat(indent, depth)
	for _, m := range sorted {
		out.blank()
		out.line(pad + yamlKey(m.Name) + ":")
		emitMutationBody(out, depth+1, m)
	}
}

func emitMutationBody(out *yamlWriter, depth int, m *model.Mutation) {
	pad := strings.Repeat(indent, depth)
	if m.Structural {
		out.line(pad + "structural: true")
	}
	if len(m.Deps) > 0 {
		out.line(pad + "deps:")
		for _, d := range m.Deps {
			if d.ReapplyOn == nil {
				out.line(pad + indent + "- " + yamlScalar(d.Pattern))
			} else {
				out.line(pad + indent + "- {name: " + yamlScalar(d.Pattern) + ", reapply_on: " + flowTriggers(d.ReapplyOn) + "}")
			}
		}
	}
	if m.ReapplyOn != nil {
		out.line(pad + "reapply_on: " + flowTriggers(m.ReapplyOn))
	}
	if len(m.Statements) == 0 {
		return
	}
	out.line(pad + "sql:")
	for _, st := range m.Statements {
		out.statement(depth+1, st)
	}
}

func flowTriggers(list []model.Trigger) string {
	parts := make([]string, len(list))
	for i, t := range list {
		parts[i] = string(t)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// ///////////////////////////////////////////////////////////
// A small yaml emitter, written by hand because SQL has to come out as literal block
// scalars to stay readable, which is the whole point of a collected file.

type yamlWriter struct {
	w   io.Writer
	err error
}

const indent = "  "

func (y *yamlWriter) write(s string) {
	if y.err != nil {
		return
	}
	_, y.err = io.WriteString(y.w, s)
}

func (y *yamlWriter) line(s string) { y.write(s + "\n") }

func (y *yamlWriter) blank() { y.write("\n") }

func (y *yamlWriter) field(depth int, key string, value string) {
	y.line(strings.Repeat(indent, depth) + key + ": " + yamlScalar(value))
}

func (y *yamlWriter) fieldInt(depth int, key string, value int) {
	y.line(strings.Repeat(indent, depth) + key + ": " + strconv.Itoa(value))
}

// stringList writes key: [...] unless values is empty.
func (y *yamlWriter) stringList(depth int, key string, values []string) {
	if len(values) > 0 {
		y.stringListForce(depth, key, values)
	}
}

// stringListForce writes key: [...] even when values is empty.
func (y *yamlWriter) stringListForce(depth int, key string, values []string) {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = yamlScalar(v)
	}
	y.line(strings.Repeat(indent, depth) + key + ": [" + strings.Join(quoted, ", ") + "]")
}

// statement writes one entry of a sql list: a plain string when dmut infers the same
// statement from it, a mapping otherwise.
func (y *yamlWriter) statement(depth int, st model.Statement) {
	pad := strings.Repeat(indent, depth)
	if len(st.Settings) == 0 && st.Data == nil {
		if inferred, err := model.Infer(st.Up); err == nil && inferred.Kind == st.Kind && inferred.Down == st.Down {
			y.write(pad + "- ")
			y.block(depth+1, st.Up)
			return
		}
	}

	key := map[model.Kind]string{model.KindReversible: "up", model.KindSeed: "seed", model.KindCheck: "ensure"}[st.Kind]
	if inferred, err := model.Infer(st.Up); st.Kind == model.KindReversible && err == nil && inferred.Down == st.Down {
		key = "auto"
	}
	y.write(pad + "- " + key + ": ")
	y.block(depth+2, st.Up)
	if key == "up" {
		y.write(pad + indent + "down: ")
		y.block(depth+2, st.Down)
	}
	if len(st.Settings) > 0 {
		parts := make([]string, len(st.Settings))
		for i, s := range st.Settings {
			if s.Tracked && !s.Saved {
				parts[i] = yamlScalar(s.Name)
				continue
			}
			parts[i] = "{name: " + yamlScalar(s.Name) + ", tracked: " + strconv.FormatBool(s.Tracked) + ", saved: " + strconv.FormatBool(s.Saved) + "}"
		}
		y.line(pad + indent + "settings: [" + strings.Join(parts, ", ") + "]")
	}
	if st.Data != nil {
		if len(st.Data) == 0 {
			y.line(pad + indent + "data: []")
			return
		}
		y.line(pad + indent + "data:")
		for _, row := range st.Data {
			cells := make([]string, len(row))
			for i, v := range row {
				cells[i] = dataCell(v)
			}
			y.line(pad + indent + indent + "- [" + strings.Join(cells, ", ") + "]")
		}
	}
}

// dataCell writes a value of a seed's data. A string that would be read back as a number or a
// boolean is quoted, so that its text stays as written.
func dataCell(v *string) string {
	if v == nil {
		return "null"
	}
	s := *v
	if s != "" && strings.ContainsRune("0123456789+-.", rune(s[0])) {
		return quoteYaml(s)
	}
	return yamlScalar(s)
}

// block writes a value as a literal block scalar, starting on the line that the caller opened.
func (y *yamlWriter) block(depth int, value string) {
	value = strings.TrimRight(value, " \t\n")
	pad := strings.Repeat(indent, depth)

	// A first line that starts with a space needs an explicit indentation indicator.
	if strings.HasPrefix(value, " ") || strings.HasPrefix(value, "\t") {
		y.line("|" + strconv.Itoa(len(pad)) + "-")
	} else {
		y.line("|-")
	}
	for _, l := range strings.Split(value, "\n") {
		if strings.TrimSpace(l) == "" {
			y.line("")
			continue
		}
		y.line(pad + l)
	}
}

// yamlScalar quotes a value when it cannot be written plainly.
func yamlScalar(s string) string {
	if isPlainSafe(s) {
		return s
	}
	return quoteYaml(s)
}

func quoteYaml(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// yamlKey quotes a mapping key when needed.
func yamlKey(s string) string {
	if !isPlainSafe(s) {
		return quoteYaml(s)
	}
	return s
}

func isPlainSafe(s string) bool {
	if s == "" || strings.TrimSpace(s) != s {
		return false
	}
	if strings.ContainsAny(s, ":#{}[]&*!|>'\"%@`,\n\t") {
		return false
	}
	switch s[0] {
	case '-', '?', '.':
		return false
	}
	switch strings.ToLower(s) {
	case "true", "false", "null", "yes", "no", "on", "off", "~":
		return false
	}
	return true
}

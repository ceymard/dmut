package mutations

import (
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/samber/oops"
)

// DmutNamespace is where dmut keeps its own bookkeeping. It ships embedded in the
// binary and must never be written out by Collect.
const DmutNamespace = "__dmut__"

// Collect writes every mutation of every namespace back out as yaml, one document
// per (namespace, revision), sorted by name so the result is stable and diffable.
//
// The output is meant to be read back by dmut : statements whose down can be
// inferred are written as plain strings, the rest as up/down pairs, and the
// dependencies that dmut derives from dotted names are left out.
func Collect(namespaces *MutationNamespace, w io.Writer) error {
	out := &yamlWriter{w: w}

	first := true
	for _, ns_name := range namespaces.Keys() {
		if ns_name == DmutNamespace {
			continue
		}

		revisions, ok := namespaces.Map.Get(ns_name)
		if !ok {
			return oops.In("mutations").With("namespace", ns_name).Errorf("no revision sequence found")
		}

		var revision_numbers []int
		for revision := range revisions.Revisions {
			revision_numbers = append(revision_numbers, revision)
		}
		sort.Ints(revision_numbers)

		for _, revision := range revision_numbers {
			set := revisions.Revisions[revision]

			if !first {
				out.line("---")
			}
			first = false

			if ns_name != "" {
				out.field(0, "__namespace", ns_name)
			}
			out.fieldInt(0, "__revision", revision)

			var names []string
			for mut := range set.AllMutations() {
				names = append(names, mut.Name)
			}
			sort.Strings(names)

			for _, name := range names {
				mut, _ := set.GetMutation(name)
				out.blank()
				out.line(yamlKey(name) + ":")
				out.stringList(1, "needs", mut.declaredNeeds(set, mut.Needs))
				out.statements(1, "sql", mut.Sql, mut.Sql != nil)
				out.stringList(1, "meta_needs", mut.declaredNeeds(set, mut.MetaNeeds))
				out.statements(1, "meta", mut.Meta, mut.Meta != nil)
				out.stringList(1, "new_needs", mut.declaredNeeds(set, mut.NewNeeds))
				// new_sql is meaningful when empty : it retires a mutation.
				out.statements(1, "new_sql", mut.NewSql, mut.NewSql != nil)
			}
		}
	}

	return out.err
}

// declaredNeeds strips the dependencies that ResolveDependencies derived from the
// dotted name, so that collecting an already-collected file is a no-op.
func (mut *Mutation) declaredNeeds(set *MutationSet, needs []string) []string {
	implicit := make(map[string]bool)
	components := mut.NameComponents()
	for i := 0; i < len(components)-1; i++ {
		ancestor := strings.Join(components[:i+1], ".")
		if set.HasMutation(ancestor) {
			implicit[ancestor] = true
		}
	}

	var res []string
	seen := make(map[string]bool)
	for _, need := range needs {
		if implicit[need] || seen[need] {
			continue
		}
		seen[need] = true
		res = append(res, need)
	}
	return res
}

// isAutoDown reports whether stmt.Down is exactly what dmut would have inferred,
// in which case the statement can be written back as a plain string.
func isAutoDown(stmt MutationStatement) bool {
	down, no_down, err := AutoDowner.ParseAndGetDefault(stmt.Up)
	if err != nil {
		return false
	}
	if no_down {
		return stmt.Down == ""
	}
	return down == stmt.Down
}

// ///////////////////////////////////////////////////////////
// A small yaml emitter. We write it by hand because SQL has to come out as literal
// block scalars to stay readable, which is the whole point of a collected file.

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

func (y *yamlWriter) line(s string) {
	y.write(s + "\n")
}

func (y *yamlWriter) blank() {
	y.write("\n")
}

func (y *yamlWriter) field(depth int, key string, value string) {
	y.line(strings.Repeat(indent, depth) + key + ": " + yamlScalar(value))
}

func (y *yamlWriter) fieldInt(depth int, key string, value int) {
	y.line(strings.Repeat(indent, depth) + key + ": " + strconv.Itoa(value))
}

func (y *yamlWriter) stringList(depth int, key string, values []string) {
	if len(values) == 0 {
		return
	}
	var quoted []string
	for _, v := range values {
		quoted = append(quoted, yamlScalar(v))
	}
	y.line(strings.Repeat(indent, depth) + key + ": [" + strings.Join(quoted, ", ") + "]")
}

// statements writes a sql/meta/new_sql block. emit_empty keeps `key: []` in the
// output, which new_sql needs to mean "this mutation is retired".
func (y *yamlWriter) statements(depth int, key string, stmts []MutationStatement, emit_empty bool) {
	if len(stmts) == 0 {
		if emit_empty {
			y.line(strings.Repeat(indent, depth) + key + ": []")
		}
		return
	}

	pad := strings.Repeat(indent, depth)
	y.line(pad + key + ":")

	for _, stmt := range stmts {
		if isAutoDown(stmt) {
			y.write(pad + indent + "- ")
			y.block(depth+2, stmt.Up)
			continue
		}
		y.write(pad + indent + "- up: ")
		y.block(depth+3, stmt.Up)
		if strings.TrimSpace(stmt.Down) == "" {
			// No down at all : COMMENT ON and friends.
			y.line(pad + indent + indent + "down:")
			continue
		}
		y.write(pad + indent + indent + "down: ")
		y.block(depth+3, stmt.Down)
	}
}

// block writes a value as a literal block scalar, starting on the line that has
// already been opened by the caller.
func (y *yamlWriter) block(depth int, value string) {
	value = strings.TrimRight(value, " \t\n")
	pad := strings.Repeat(indent, depth)

	// A first line that starts with a space needs an explicit indentation indicator,
	// otherwise yaml cannot tell content from indentation.
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

// yamlKey quotes a mapping key when needed. A mutation whose name starts with __
// would be read back as a directive, so those are always quoted.
func yamlKey(s string) string {
	if strings.HasPrefix(s, "__") || !isPlainSafe(s) {
		return quoteYaml(s)
	}
	return s
}

func isPlainSafe(s string) bool {
	if s == "" {
		return false
	}
	// Leading/trailing spaces and yaml indicators all need quoting.
	if strings.TrimSpace(s) != s {
		return false
	}
	if strings.ContainsAny(s, ":#{}[]&*!|>'\"%@`,\n\t") {
		return false
	}
	switch s[0] {
	case '-', '?', '.':
		return false
	}
	// Things that would be read back as something other than a string.
	switch strings.ToLower(s) {
	case "true", "false", "null", "yes", "no", "on", "off", "~":
		return false
	}
	return true
}

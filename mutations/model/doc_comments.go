package model

import (
	"fmt"
	"strings"

	lexer "github.com/alecthomas/participle/v2/lexer"
)

var tokDoc = SqlLexer.Symbols()["DocComment"]

// Doc comments are written right before what they describe: a CREATE, or a column or a named
// constraint in a column list. The grammar of auto_statements.go captures them; docStatements
// turns what it captured into COMMENT ON statements, which the loader appends at the end of the
// mutation.

// hasDocMarker is a cheap test, run before lexing, for text that may hold a doc comment.
func hasDocMarker(sql string) bool {
	return strings.Contains(sql, "--?") || strings.Contains(sql, "/*?")
}

// docStatements reads the doc groups of a parse and returns the COMMENT ON statements they stand
// for, in the order they were written. Every doc comment of toks must be used by one of them.
func docStatements(res state, toks []lexer.Token) ([]string, error) {
	claimed := map[int]bool{}
	var comments []string
	var pending []lexer.Token
	var owner, table string
	bind := func(target string) {
		if len(pending) == 0 {
			return
		}
		texts := make([]string, len(pending))
		for i, t := range pending {
			claimed[t.Pos.Offset] = true
			texts[i] = docText(t.Value)
		}
		comments = append(comments, "COMMENT ON "+target+" IS "+sqlString(strings.Join(texts, "\n")))
		pending = nil
	}

	if res.isMatch() {
		for i := 0; i < len(res.results); i++ {
			r := res.results[i]
			switch r.group {
			case "doc":
				pending = append(pending, r.value)
			case "spec":
				j := i
				for j+1 < len(res.results) && res.results[j+1].group == "spec" {
					j++
				}
				spec := render(res.results[i:j+1], "spec")
				if strings.HasPrefix(strings.ToLower(spec), "user mapping") {
					return nil, fmt.Errorf("a user mapping can't be commented")
				}
				owner = res.results[j].value.Value
				bind(spec)
				i = j
			case "tbl":
				table = r.value.Value
			case "col":
				bind("COLUMN " + table + "." + r.value.Value)
			case "column":
				bind("COLUMN " + owner + "." + r.value.Value)
			case "constraint":
				bind("CONSTRAINT " + r.value.Value + " ON " + owner)
			case "unnamed":
				if len(pending) > 0 {
					return nil, fmt.Errorf("the doc comment at line %d can't document an unnamed constraint ; give it a name with CONSTRAINT", pending[0].Pos.Line)
				}
			}
		}
	}
	for _, t := range toks {
		if t.Type == tokDoc && !claimed[t.Pos.Offset] {
			return nil, fmt.Errorf("the doc comment at line %d, column %d documents nothing ; it goes right before a CREATE, an ALTER TABLE ... ADD COLUMN, or a column or a named constraint of a column list", t.Pos.Line, t.Pos.Column)
		}
	}
	return comments, nil
}

// docText is the text of a doc comment without its markers.
func docText(raw string) string {
	if rest, ok := strings.CutPrefix(raw, "/*?"); ok {
		return dedent(strings.TrimSpace(strings.TrimSuffix(rest, "*/")))
	}
	text := strings.TrimRight(strings.TrimPrefix(raw, "--?"), "\r\n")
	return strings.TrimPrefix(text, " ")
}

// dedent removes the indentation that every line of text after the first has in common.
func dedent(text string) string {
	lines := strings.Split(text, "\n")
	common := -1
	for _, l := range lines[1:] {
		if strings.TrimSpace(l) == "" {
			continue
		}
		if n := len(l) - len(strings.TrimLeft(l, " \t")); common < 0 || n < common {
			common = n
		}
	}
	if common <= 0 {
		return text
	}
	for i, l := range lines[1:] {
		lines[i+1] = l[min(common, len(l)):]
	}
	return strings.Join(lines, "\n")
}

// sqlString quotes text as a string literal.
func sqlString(text string) string {
	return "'" + strings.ReplaceAll(text, "'", "''") + "'"
}

// cutDocs removes the doc comments from sql. A comment alone on its line takes its indentation with it.
func cutDocs(sql string, docs []lexer.Token) string {
	var b strings.Builder
	last := 0
	for _, d := range docs {
		start, end := d.Pos.Offset, d.Pos.Offset+len(d.Value)
		s := start
		for s > last && (sql[s-1] == ' ' || sql[s-1] == '\t') {
			s--
		}
		b.WriteString(sql[last:s])
		if s > 0 && sql[s-1] != '\n' {
			// Something precedes it on the line: keep its neighbours apart.
			b.WriteByte(' ')
		}
		last = end
	}
	b.WriteString(sql[last:])
	return b.String()
}

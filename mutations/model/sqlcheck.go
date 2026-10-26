package model

import (
	"strings"

	"github.com/alecthomas/participle/v2/lexer"
	"github.com/samber/oops"
)

var (
	tokSemicolon = sqlLexer.Symbols()["Semicolon"]
	tokMultiOpen = sqlLexer.Symbols()["MultiStart"]
	tokMultiStop = sqlLexer.Symbols()["MultiStop"]
	tokRest      = sqlLexer.Symbols()["Rest"]

	ignoredTokens = map[lexer.TokenType]bool{
		sqlLexer.Symbols()["whiteSpace"]:        true,
		sqlLexer.Symbols()["multiCommentStart"]: true,
		sqlLexer.Symbols()["multiCommentStop"]:  true,
		sqlLexer.Symbols()["char"]:              true,
	}
)

// objectKinds are the objects a default mutation may create, drop and alter: they hold no rows.
var objectKinds = map[string]bool{
	"function": true, "procedure": true, "aggregate": true, "view": true,
	"trigger": true, "policy": true, "rule": true,
}

// dataStatements are the first words of the statements that only read or write rows.
var dataStatements = map[string]bool{
	"insert": true, "update": true, "delete": true, "merge": true, "truncate": true,
	"copy": true, "select": true, "with": true, "do": true, "call": true,
}

// sqlStatements cuts sql into statements of meaningful tokens on top-level semicolons.
// Dollar-quoted bodies and `begin atomic ... end` function bodies do not split.
func sqlStatements(sql string) ([][]lexer.Token, error) {
	lx, err := sqlLexer.LexString("", sql)
	if err != nil {
		return nil, err
	}
	var out [][]lexer.Token
	var cur []lexer.Token
	depth, caseDepth := 0, 0
	atomic := false
	for {
		tok, err := lx.Next()
		if err != nil {
			return nil, err
		}
		if tok.EOF() {
			break
		}
		switch {
		case ignoredTokens[tok.Type]:
			continue
		case tok.Type == tokMultiOpen:
			depth++
		case tok.Type == tokMultiStop:
			depth--
		case tok.Type == tokSemicolon && depth == 0 && !atomic:
			if len(cur) > 0 {
				out = append(out, cur)
			}
			cur = nil
			continue
		}
		cur = append(cur, tok)
		if depth != 0 {
			continue
		}
		// A SQL-standard function body ends at the `end` that closes `begin atomic`.
		switch strings.ToLower(tok.Value) {
		case "atomic":
			if n := len(cur); n >= 2 && strings.EqualFold(cur[n-2].Value, "begin") && isFunctionDefinition(cur) {
				atomic = true
			}
		case "case":
			if atomic {
				caseDepth++
			}
		case "end":
			if atomic {
				if caseDepth > 0 {
					caseDepth--
				} else {
					atomic = false
				}
			}
		}
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out, nil
}

func isFunctionDefinition(toks []lexer.Token) bool {
	w := words(toks, 5)
	if len(w) < 2 || w[0] != "create" {
		return false
	}
	i := 1
	if w[i] == "or" && i+1 < len(w) {
		i += 2
	}
	return i < len(w) && (w[i] == "function" || w[i] == "procedure")
}

// words returns the first n tokens of a statement, lowercased.
func words(toks []lexer.Token, n int) []string {
	var out []string
	for _, tok := range toks {
		if len(out) == n {
			break
		}
		out = append(out, strings.ToLower(tok.Value))
	}
	return out
}

// describe names a statement by its first words, for error messages.
func describe(toks []lexer.Token) string {
	return strings.Join(words(toks, 3), " ")
}

// checkSQL refuses transaction control in every mutation, and in a default mutation
// anything that is not on the default list.
func checkSQL(sql string, structural bool) error {
	stmts, err := sqlStatements(sql)
	if err != nil {
		return oops.In("model").Wrapf(err, "can't read statement")
	}
	for _, toks := range stmts {
		if isTransactionControl(toks) {
			return oops.In("model").Errorf("%q controls the transaction, which dmut owns", describe(toks))
		}
		if !structural && !defaultAllowed(toks) {
			return oops.In("model").Errorf("%q is not allowed in a default mutation ; set structural: true", describe(toks))
		}
	}
	return nil
}

func isTransactionControl(toks []lexer.Token) bool {
	w := words(toks, 2)
	if len(w) == 0 {
		return false
	}
	switch w[0] {
	case "begin", "commit", "end", "rollback", "savepoint", "release":
		return true
	case "start", "prepare":
		return len(w) > 1 && w[1] == "transaction"
	}
	return false
}

func defaultAllowed(toks []lexer.Token) bool {
	w := words(toks, 8)
	if len(w) == 0 {
		return true
	}
	if dataStatements[w[0]] {
		return true
	}
	switch w[0] {
	case "grant", "revoke":
		return true
	case "comment":
		return len(w) > 1 && w[1] == "on"
	case "create":
		i := 1
		if i+1 < len(w) && w[i] == "or" && w[i+1] == "replace" {
			i += 2
		}
		for i < len(w) && (w[i] == "temp" || w[i] == "temporary" || w[i] == "recursive" || w[i] == "constraint") {
			i++
		}
		if i >= len(w) {
			return false
		}
		if w[i] == "type" {
			// Only composite types: create type name as ( ... )
			return i+3 < len(w) && w[i+2] == "as" && w[i+3] == "("
		}
		return objectKinds[w[i]]
	case "drop":
		return len(w) > 1 && (objectKinds[w[1]] || w[1] == "type")
	case "alter":
		if len(w) < 2 {
			return false
		}
		switch w[1] {
		case "default":
			return len(w) > 2 && w[2] == "privileges"
		case "table":
			return alterTableAllowed(toks)
		}
		return objectKinds[w[1]]
	}
	return false
}

// alterTableAllowed accepts ALTER TABLE only when every comma-separated action changes no
// stored data: row level security, trigger enabling and column defaults.
func alterTableAllowed(toks []lexer.Token) bool {
	i := 2
	lower := func(j int) string {
		if j >= len(toks) {
			return ""
		}
		return strings.ToLower(toks[j].Value)
	}
	if lower(i) == "if" && lower(i+1) == "exists" {
		i += 2
	}
	if lower(i) == "only" {
		i++
	}
	if i >= len(toks) {
		return false
	}
	i++ // the table name
	if lower(i) == "*" {
		i++
	}

	// Split on commas outside brackets: default expressions can contain some.
	depth := 0
	var action []string
	actions := [][]string{}
	for ; i < len(toks); i++ {
		tok := toks[i]
		if tok.Type == tokRest {
			switch tok.Value {
			case "(", "[", "{":
				depth++
			case ")", "]", "}":
				depth--
			case ",":
				if depth == 0 {
					actions = append(actions, action)
					action = nil
					continue
				}
			}
		}
		action = append(action, strings.ToLower(tok.Value))
	}
	actions = append(actions, action)

	for _, a := range actions {
		if !alterTableActionAllowed(a) {
			return false
		}
	}
	return true
}

func alterTableActionAllowed(a []string) bool {
	has := func(prefix ...string) bool {
		if len(a) < len(prefix) {
			return false
		}
		for i, p := range prefix {
			if a[i] != p {
				return false
			}
		}
		return true
	}
	switch {
	case has("enable", "row", "level", "security"), has("disable", "row", "level", "security"),
		has("force", "row", "level", "security"), has("no", "force", "row", "level", "security"):
		return true
	case has("enable", "trigger"), has("disable", "trigger"),
		has("enable", "replica", "trigger"), has("enable", "always", "trigger"):
		return true
	case has("alter") && len(a) > 1 && a[1] != "constraint":
		// ALTER [COLUMN] name SET DEFAULT ... | DROP DEFAULT
		i := 1
		if a[i] == "column" {
			i++
		}
		i++ // the column name
		return i+1 < len(a) && (a[i] == "set" || a[i] == "drop") && a[i+1] == "default"
	}
	return false
}

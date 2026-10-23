package mutations

import (
	"strings"

	lexer "github.com/alecthomas/participle/v2/lexer"
	"github.com/samber/oops"
)

var (
	tokSemicolon = SqlLexer.Symbols()["Semicolon"]
	tokMultiOpen = SqlLexer.Symbols()["MultiStart"]
	tokMultiStop = SqlLexer.Symbols()["MultiStop"]
	tokID        = SqlLexer.Symbols()["Id"]
	tokRest      = SqlLexer.Symbols()["Rest"]

	// Comment and whitespace tokens carry no meaning for classification.
	ignoredTokens = map[lexer.TokenType]bool{
		SqlLexer.Symbols()["whiteSpace"]:        true,
		SqlLexer.Symbols()["multiCommentStart"]: true,
		SqlLexer.Symbols()["multiCommentStop"]:  true,
		SqlLexer.Symbols()["char"]:              true,
	}
)

// glueObjectKinds are the object kinds whose CREATE, DROP and ALTER may appear in glue:
// they describe behaviour and hold no data.
var glueObjectKinds = map[string]bool{
	"function": true, "procedure": true, "aggregate": true, "view": true,
	"trigger": true, "policy": true, "rule": true,
}

// glueAlterKinds are the ALTER targets allowed in glue (OWNER TO and renames included).
// ALTER TRIGGER / POLICY / RULE only rename or retarget, they never touch rows.
var glueAlterKinds = glueObjectKinds

// splitStatements cuts sql into its statements (as meaningful tokens, no whitespace or
// comments) on top-level semicolons. A semicolon inside a dollar-quoted body does not split.
func splitStatements(sql string) ([][]lexer.Token, error) {
	tokens, err := split(sql)
	if err != nil {
		return nil, err
	}
	var out [][]lexer.Token
	var cur []lexer.Token
	depth := 0
	for _, tok := range tokens {
		switch {
		case ignoredTokens[tok.Type]:
			continue
		case tok.Type == tokMultiOpen:
			depth++
		case tok.Type == tokMultiStop:
			depth--
		case tok.Type == tokSemicolon && depth == 0:
			if len(cur) > 0 {
				out = append(out, cur)
			}
			cur = nil
			continue
		}
		cur = append(cur, tok)
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out, nil
}

// words lowercases the leading keyword tokens of a statement ; a quoted identifier never
// matches a keyword, so it is kept verbatim (quotes included).
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

// describeStatement names a statement by its first words, for error messages.
func describeStatement(toks []lexer.Token) string {
	return strings.Join(words(toks, 3), " ")
}

// checkGlueSQL returns an error if any statement of sql is not on the glue whitelist.
func checkGlueSQL(sql string) error {
	stmts, err := splitStatements(sql)
	if err != nil {
		return oops.In("mutations").Wrapf(err, "can't read statement")
	}
	for _, toks := range stmts {
		if !glueAllowed(toks) {
			return oops.In("mutations").With("statement", sql).
				Errorf("%q is not allowed in glue (glue may only define functions, procedures, aggregates, views, triggers, policies, rules, grants and comments) ; move it to core", describeStatement(toks))
		}
	}
	return nil
}

func glueAllowed(toks []lexer.Token) bool {
	w := words(toks, 8)
	if len(w) == 0 {
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
		return i < len(w) && glueObjectKinds[w[i]]
	case "drop":
		return len(w) > 1 && glueObjectKinds[w[1]]
	case "alter":
		if len(w) < 2 {
			return false
		}
		if w[1] == "default" {
			return len(w) > 2 && w[2] == "privileges"
		}
		if w[1] == "table" {
			return glueAlterTableAllowed(toks)
		}
		return glueAlterKinds[w[1]]
	}
	return false
}

// glueAlterTableAllowed accepts ALTER TABLE only when every comma-separated action is one
// that changes no stored data : row level security, trigger enabling, column defaults.
func glueAlterTableAllowed(toks []lexer.Token) bool {
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

	// Split the actions on commas outside parentheses/brackets (default expressions can contain some).
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
		if !glueAlterTableActionAllowed(a) {
			return false
		}
	}
	return true
}

func glueAlterTableActionAllowed(a []string) bool {
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

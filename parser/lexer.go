// A special lexer since there is no support for backreference in go regexp
package dmutparser

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/alecthomas/participle/lexer"
)

var eolBytes = []byte("\n")

type regexpToken struct {
	regexp   *regexp.Regexp
	name     string
	rune     rune
	skip     bool
	endmatch *string
}

type regexpDefinition2 struct {
	defs []regexpToken
}

func Regexp2Lexer() *regexpDefinition2 {

	return &regexpDefinition2{
		defs: make([]regexpToken, 0, 16),
	}
}

func (rd *regexpDefinition2) Token(name string, reg string) *regexpDefinition2 {
	var nt = regexpToken{
		regexp.MustCompile(("^(?:" + reg + ")")),
		name,
		rune(-2 - len(rd.defs)),
		name[0] == '_',
		nil,
	}
	rd.defs = append(rd.defs, nt)
	// FIXME : need some accel map to make search faster !
	return rd
}

func (rd *regexpDefinition2) EndMatch(endmatch string) *regexpDefinition2 {
	rd.defs[len(rd.defs)-1].endmatch = &endmatch
	return rd
}

func (rd *regexpDefinition2) Lex(filename string, reader io.Reader) (lexer.Lexer, error) {
	// Read up all the contents.
	// FIXME: it would be so much better if we could do some buffering !
	// Note : shoul
	contents, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	return &regexp2Lexer{
		rd.defs,
		lexer.Position{
			Filename: lexer.NameOfReader(reader),
			Column:   1,
			Line:     1,
		},
		contents,
	}, nil
}

func (rd *regexpDefinition2) Symbols() map[string]rune {
	res := make(map[string]rune)
	for _, def := range rd.defs {
		res[def.name] = def.rune
	}
	res["EOF"] = lexer.EOF
	return res
}

//////////////////////////////////////////////////////////////

type regexp2Lexer struct {
	defs   []regexpToken
	pos    lexer.Position
	buffer []byte
}

// func (r *regexpLexer) Next() (lexer.Token, error) {
func (rl *regexp2Lexer) Next() (lexer.Token, error) {
	// We will try to match
	defs := rl.defs

	if len(rl.buffer) == 0 {
		return lexer.EOFToken(rl.pos), nil
	}

	// maybe start by picking up a slice of where we really want to find stuff...
nextToken:
	for _, def := range defs {
		loc := def.regexp.FindIndex(rl.buffer)

		// If we didn't find a match, keep going with the next token
		if loc == nil {
			continue
		}

		// This is the text that we matched
		match := rl.buffer[:loc[1]]

		// This is a two-part
		if def.endmatch != nil {
			// We are now going to look for
			endregexp := regexp.MustCompile(
				`^(.|\n)*?` +
					regexp.QuoteMeta(strings.ReplaceAll(*def.endmatch, "\\1", string(match))),
			)

			endloc := endregexp.FindIndex(rl.buffer[loc[1]:])
			if endloc == nil {
				// log.Print("????", string(match), "   ", endregexp)
				// This did not match, so maybe we should send some error like
				// unterminated ? For now the error handling may not be nice...
				continue
			}

			// Put the whole of the match into the matched text.
			match = rl.buffer[:loc[1]+endloc[1]]
			loc[1] += endloc[1] // we have to cheat a bit.
		}

		// Now we have the match sorted out, compute the number of lines it consumes
		// and the new column index
		var curpos = rl.pos
		rl.pos.Offset += loc[1]
		lines := bytes.Count(match, eolBytes)
		rl.pos.Line += lines

		if lines == 0 {
			rl.pos.Column += utf8.RuneCount(match)
		} else {
			rl.pos.Column = utf8.RuneCount(match[bytes.LastIndex(match, eolBytes):])
		}

		// update the buffer and make it smaller
		rl.buffer = rl.buffer[loc[1]:]

		// log.Print(def.name, ": ", string(match))
		if def.skip {
			// This token is skippable, so we're not going to return it
			goto nextToken
		}

		// We can now build the token !
		token := lexer.Token{
			Type:  def.rune,
			Pos:   curpos,
			Value: string(match),
		}
		// log.Print(token)
		return token, nil
	}

	if len(rl.buffer) == 0 {
		return lexer.EOFToken(rl.pos), nil
	}

	// If we get here, it most likely means that we didn't match anything in the input, so we'll return an error.
	rn, _ := utf8.DecodeRune(rl.buffer)
	return lexer.Token{}, fmt.Errorf("invalid token %q at %v", rn, rl.pos)
}

////////////////////////////////////////////////////////////

var (
	// SqlLexer is a lexer for sql
	SqlLexer = Regexp2Lexer().Token(
		"_WhiteSpace", `\s+|--[^\n]*\n?|/\*(.|\n)*?\*/`,
	).Token(
		"Semicolon", `;`,
	).Token(
		"MultilineString", `\$[a-zA-Z_0-9]*\$`,
	).EndMatch(`\1`).Token(
		"SqlId", `(?:"(""|[^"])*"|[@$a-zA-Z_][\w$]*|\[[^\]]+\])(?:\.(?:"(""|[^"])*"|[@$a-zA-Z_][\w$]*|\[[^\]]+\]))*`,
	).Token(
		"DotStar", `\.\*`,
	).Token(
		"Number", `[-+]?\d*\.?\d+([eE][-+]?\d+)?`,
	).Token(
		"String", `'(?:''|[^'])*'`,
	).Token(
		"Rest", `::|<>|!=|<=|>=|[-+?!~|^#*/%,.(&)=<>:\[\]]`,
	)
)

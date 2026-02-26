package mutations

import (
	"fmt"
	"strings"
	"unicode"

	lexer "github.com/alecthomas/participle/v2/lexer"
)

var (
	RestPattern = `\d+|::|<>|!=|<=|>=|[-+?!~|^#*/%,.()=<>:\[\]]`
	// The sql lexer
	SqlLexer = lexer.MustStateful(lexer.Rules{
		"Root": {
			{Name: "multiCommentStart", Pattern: `/\*`, Action: lexer.Push("MultiComment")},
			{Name: "MultiStart", Pattern: `(\$[a-zA-Z_0-9]*\$)`, Action: lexer.Push("MultilineString")},
			{Name: "whiteSpace", Pattern: `( |\s|\n)+|--[^\n]*\n?|/\*(.|\n)*?\*/`},
			{Name: "Semicolon", Pattern: `;`},
			{Name: "Id", Pattern: `(?:"(""|[^"])*"|[@$a-zA-Z_][\w$]*|\[[^\]]+\])(?:\.(?:"(""|[^"])*"|[@$a-zA-Z_][\w$]*|\[[^\]]+\]))*`},
			{Name: "String", Pattern: `'(?:''|[^'])*'`},
			{Name: "Operator", Pattern: "[+\\-*/<>=~!@#%^&|`?$]{1,63}"},
			{Name: "Rest", Pattern: RestPattern},
		},

		"MultiComment": {
			{Name: "multiCommentStart", Pattern: `/\*`, Action: lexer.Push("MultiComment")},
			{Name: "multiCommentStop", Pattern: `\*/`, Action: lexer.Pop()},
			{Name: "char", Pattern: `.|\n`},
		},

		"MultilineString": {
			{Name: "MultiStop", Pattern: `\1`, Action: lexer.Pop()},
			{Name: "char", Pattern: `.|\n`},
		},
	})
)

func split(s string) ([]lexer.Token, error) {
	lx, err := SqlLexer.LexString("", s)
	if err != nil {
		return nil, err
	}

	var tokens []lexer.Token
	for tok, _ := lx.Next(); !tok.EOF(); tok, _ = lx.Next() {
		tokens = append(tokens, tok)
	}
	return tokens, nil
}

// ///////////////////////////////////////////////

func getCombinator(s any) *combinator {
	if comb, ok := s.(*combinator); ok {
		return comb
	}
	if s, ok := s.(string); ok {
		return str(s)
	}
	panic(fmt.Errorf("unknown type: %T", s))
}

func getCombinatorSlice(s ...any) []*combinator {
	var res []*combinator
	for _, s := range s {
		res = append(res, getCombinator(s))
	}
	return res
}

type combinator struct {
	parser   simpleParser
	producer []producer
}

func (c *combinator) ParseState(s state) state {
	var new_state = c.parser.Parse(s)
	if len(c.producer) > 0 {
		var new_results []result
		var old_results = new_state.results
		new_state.results = new_results
		for _, producer := range c.producer {
			new_state = producer.act(new_state, old_results)
		}
	}
	return new_state
}

func (c *combinator) Parse(s string) (state, error) {
	tokens, err := split(s)
	if err != nil {
		return state{}, nil
	}
	return c.ParseState(state{
		file:    s,
		tokens:  tokens,
		results: []result{},
		pos:     0,
	}), nil
}

func (c *combinator) ParseAndGetDefault(s string) (string, error) {
	res, err := c.Parse(s)
	if err != nil {
		return "", err
	}
	if res.isNoMatch() {
		return "", fmt.Errorf("no match")
	}
	var acc = ""
	for _, result := range res.results {
		token := result.value
		if token.Pos.Offset > 0 && unicode.IsSpace(rune(res.file[token.Pos.Offset-1])) {
			acc += " "
		}
		acc += token.Value
	}
	return acc, nil
}

func (c *combinator) Produce(producers ...any) *combinator {
	for _, prod := range producers {
		if prod, ok := prod.(producer); ok {
			c.producer = append(c.producer, prod)
			continue
		}

		if str, ok := prod.(string); ok {
			c.producer = append(c.producer, newStringProducer("", str))
			continue
		}

		panic(fmt.Errorf("unknown producer type: %T", prod))
	}
	return c
}

// ///////////////////////////////////////////////
// Producers produce results

type producer interface {
	act(s state, old_results []result) state
}

type groupIncludeProducer struct {
	group string
	def   string
}

func (g *groupIncludeProducer) act(st state, old_results []result) state {
	found := false
	for _, result := range old_results {
		if result.group == g.group {
			found = true
			st.results = append(st.results, result)
		}
	}
	if !found && g.def != "" {
		st.results = append(st.results, result{
			group: g.group,
			value: lexer.Token{Value: g.def, Pos: lexer.Position{Offset: -1}},
		})
	}
	return st
}

func group(group string) producer {
	return &groupIncludeProducer{
		group: group,
	}
}

func groupDef(group string, def string) producer {
	return &groupIncludeProducer{
		group: group,
		def:   def,
	}
}

type stringProducer struct {
	group string
	value string
}

func (s *stringProducer) act(st state, old_results []result) state {
	st.results = append(st.results, result{
		group: s.group,
		value: lexer.Token{Value: s.value, Pos: lexer.Position{Offset: -1}},
	})
	return st
}

func newStringProducer(group string, s string) producer {
	return &stringProducer{
		group: group,
		value: s,
	}
}

type result struct {
	group string
	value lexer.Token
}

type state struct {
	file    string
	tokens  []lexer.Token
	results []result
	pos     int
}

func (s state) isNoMatch() bool {
	return s.pos == -1
}

func (s state) isMatch() bool {
	return s.pos > -1
}

func (s state) noMatch() state {
	s.pos = -1
	return s
}

func (s *state) isEOF() bool {
	return s.pos > len(s.tokens) || s.pos > -1 && s.tokens[s.pos].Type == lexer.EOF
}

type Producer interface {
	Scan(s state) state
}

type simpleParser interface {
	Parse(st state) state
}

////////////////////////////////////////////////////////////

type asisCombinator struct {
	group       string
	combinators []*combinator
}

func (a *asisCombinator) Parse(orig state) state {
	st := orig
	for _, comb := range a.combinators {
		st = comb.ParseState(st)
		if st.isNoMatch() {
			return orig.noMatch()
		}
	}
	for i := orig.pos; i < st.pos; i++ {
		st.results = append(st.results, result{
			group: a.group,
			value: orig.tokens[i],
		})
	}
	return st
}

func asIs(group string, s ...any) *combinator {
	comb := getCombinatorSlice(s...)
	return &combinator{
		parser: &asisCombinator{
			group:       group,
			combinators: comb,
		},
	}
}

// add a sequence "as-is"
func a(s ...any) *combinator {
	return asIs("", s...)
}

//////////

type seqCombinator struct {
	combinators []*combinator
}

func (s *seqCombinator) Parse(st state) state {
	for _, comb := range s.combinators {
		st = comb.ParseState(st)
		if st.isNoMatch() {
			return st
		}
	}
	return st
}

func seq(s ...any) *combinator {
	return &combinator{
		parser: &seqCombinator{
			combinators: getCombinatorSlice(s...),
		},
	}
}

///////////////////////////////

type eitherCombinator struct {
	combinators []*combinator
}

func (e *eitherCombinator) Parse(orig state) state {
	for _, comb := range e.combinators {
		st := comb.ParseState(orig)
		if st.isMatch() {
			return st
		}
	}
	return orig.noMatch()
}

func either(s ...any) *combinator {
	return &combinator{
		parser: &eitherCombinator{
			combinators: getCombinatorSlice(s...),
		},
	}
}

////////////////////////////

type zero_or_moreCombinator struct {
	combinators []*combinator
}

func (z *zero_or_moreCombinator) Parse(st state) state {

	for {
		if st.isEOF() {
			return st
		}
		orig := st
		for _, comb := range z.combinators {
			st = comb.ParseState(st)
			if st.isNoMatch() {
				return orig
			}
		}
		if orig.pos == st.pos {
			// no progress was made, should this be an error?
			return orig
		}
	}
}

func zero_or_more(s ...any) *combinator {
	return &combinator{
		parser: &zero_or_moreCombinator{
			combinators: getCombinatorSlice(s...),
		},
	}
}

////////////////////////////

type optCombinator struct {
	combinators []*combinator
}

func (o *optCombinator) Parse(orig state) state {
	st := orig
	for _, comb := range o.combinators {
		st = comb.ParseState(st)
		if !st.isMatch() {
			return orig
		}
	}
	return st
}

func opt(s ...any) *combinator {
	return &combinator{
		parser: &optCombinator{
			combinators: getCombinatorSlice(s...),
		},
	}
}

// Returns a parser that
type notCombinator struct {
	combinator *combinator
}

func (n *notCombinator) Parse(st state) state {
	newst := n.combinator.ParseState(st)
	if newst.isNoMatch() {
		return st
	}
	return st.noMatch()
}

func not(s any) *combinator {
	return &combinator{
		parser: &notCombinator{
			combinator: getCombinator(s),
		},
	}
}

/////////////////////////////

type untilCombinator struct {
	combinator *combinator
	opt_end    bool
}

func (u *untilCombinator) Parse(st state) state {
	for {
		if st.isEOF() {
			if u.opt_end {
				return st
			}
			return st.noMatch()
		}
		newst := u.combinator.ParseState(st)
		// reached until
		if newst.isMatch() {
			return st
		} else {
			st.pos++
		}
	}
}

func until(s any) *combinator {
	return &combinator{
		parser: &untilCombinator{
			combinator: getCombinator(s),
			opt_end:    false,
		},
	}
}

func until_opt(s any) *combinator {
	return &combinator{
		parser: &untilCombinator{
			combinator: getCombinator(s),
			opt_end:    true,
		},
	}
}

///////////////////////

type stringParser struct {
	value string
}

func (s *stringParser) Parse(st state) state {
	if !st.isEOF() && st.isMatch() {
		if strings.EqualFold(st.tokens[st.pos].Value, s.value) {
			st.pos++
			return st
		}
	}
	return st.noMatch()
}

////////////////////////

func str(st string) *combinator {
	return &combinator{
		parser: &stringParser{
			value: st,
		},
	}
}

////////////////////////

type untilParser struct {
	until []simpleParser
}

func (u *untilParser) Parse(s state) state {
	var res = s
	for {
		if s.isEOF() {
			return res
		}

		for _, until := range u.until {
			check := until.Parse(s)
			if check.isMatch() {
				return res
			}
		}
		res.pos++
	}

}

type tokComparer struct {
	Value string
	Type  lexer.TokenType
}

func (c *tokComparer) Compare(tok lexer.Token) bool {
	if c.Type != 0 {
		return tok.Type == c.Type
	}
	return tok.Value == c.Value
}

////////////////////////////

type tokenCombinator struct {
	tok lexer.TokenType
}

func (t *tokenCombinator) Parse(st state) state {
	if st.isEOF() {
		return st
	}
	if st.tokens[st.pos].Type == t.tok {
		st.pos++
		return st
	}
	return st.noMatch()
}

func token(group string, tok lexer.TokenType) *combinator {
	return &combinator{
		parser: &tokenCombinator{
			tok: tok,
		},
	}
}

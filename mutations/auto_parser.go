package mutations

import (
	"strings"
	"unicode"

	lexer "github.com/alecthomas/participle/v2/lexer"
	"github.com/samber/oops"
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
			{Name: "Id", Pattern: `(?:"(""|[^"])*"|[$a-zA-Z_][\w$]*|\[[^\]]+\])(?:\.(?:"(""|[^"])*"|[@$a-zA-Z_][\w$]*|\[[^\]]+\]))*`},
			{Name: "Operator", Pattern: "[+\\-*/<>=~!@#%^&|`?$]{1,63}"},
			{Name: "String", Pattern: `'(?:''|[^'])*'`},
			{Name: "Rest", Pattern: RestPattern},
		},

		"MultiComment": {
			{Name: "multiCommentStart", Pattern: `/\*`, Action: lexer.Push("MultiComment")},
			{Name: "multiCommentStop", Pattern: `\*/`, Action: lexer.Pop()},
			{Name: "char", Pattern: `.|\n`},
		},

		"MultilineString": {
			{Name: "MultiStop", Pattern: `\1`, Action: lexer.Pop()},
			{Name: "Char", Pattern: `.|\n`},
		},
	})
)

func split(s string) ([]lexer.Token, error) {
	lx, err := SqlLexer.LexString("", s)
	if err != nil {
		return nil, err
	}

	var tokens []lexer.Token
	for {
		tok, err := lx.Next()
		if err != nil {
			return nil, err
		}
		if tok.EOF() {
			break
		}
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
	panic(oops.Errorf("unknown type: %T", s))
}

func getCombinatorSlice(s ...any) []*combinator {
	var res []*combinator
	for _, s := range s {
		res = append(res, getCombinator(s))
	}
	return res
}

type combinator struct {
	parser    simpleParser
	producers []producer
}

func (c *combinator) ParseState(s state) state {
	var new_state = c.parser.Parse(s)
	if len(c.producers) > 0 && new_state.isMatch() {
		var old_r []result
		var new_results []result
		for _, result := range new_state.results {
			if result.pos <= s.pos {
				old_r = append(old_r, result)
			} else {
				new_results = append(new_results, result)
			}
		}
		// var old_results = new_state.results
		new_state.results = old_r
		for _, producer := range c.producers {
			new_state = producer.act(new_state, new_results)
		}
	}
	return new_state
}

func (c *combinator) Produce(producers ...any) *combinator {
	for _, prod := range producers {
		if prod, ok := prod.(producer); ok {
			c.producers = append(c.producers, prod)
			continue
		}

		if str, ok := prod.(string); ok {
			c.producers = append(c.producers, newStringProducer("", str))
			continue
		}

		panic(oops.Errorf("unknown producer type: %T", prod))
	}
	return c
}

func (c *combinator) Parse(s string) (state, error) {
	tokens, err := split(s)
	if err != nil {
		return state{}, err
	}
	return c.ParseState(state{
		file:    s,
		tokens:  tokens,
		results: []result{},
		pos:     0,
	}), nil
}

func wantSpace(s string) (want_left bool, want_right bool) {
	switch s {
	case "(", "[", "{", ")", "]", "}", "::", ".":
		return false, false
	case ",", ":", ";":
		return false, true
	default:
		return true, true
	}
}

func (c *combinator) ParseAndGetDefault(s string) (string, error) {
	res, err := c.Parse(s)
	if err != nil {
		return "", err
	}
	if res.isNoMatch() {
		return "", oops.In("auto_parser").With("input", s).Errorf("no match")
	}
	var acc = strings.Builder{}
	var last_want_right = false
	for _, result := range res.results {
		str := result.value.Value
		want_left, want_right := wantSpace(str)
		if last_want_right && want_left {
			acc.WriteString(" ")
		}
		last_want_right = want_right
		acc.WriteString(str)
	}
	return acc.String(), nil
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
			st.addResult("", result.value)
		}
	}
	if !found && g.def != "" {
		st.addResult("", lexer.Token{Value: g.def, Pos: lexer.Position{Offset: -1}})
	}
	return st
}

func group(group string) producer {
	return &groupIncludeProducer{
		group: group,
	}
}

func groupOrDefault(group string, def string) producer {
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
	st.addResult(s.group, lexer.Token{Value: s.value, Pos: lexer.Position{Offset: -1}})
	return st
}

func newStringProducer(group string, s string) producer {
	return &stringProducer{
		group: group,
		value: s,
	}
}

type result struct {
	pos   int
	group string
	value lexer.Token
}

func (r *result) hasSpaceBefore(st *state) bool {
	return r.value.Pos.Offset > 0 && unicode.IsSpace(rune(st.file[r.value.Pos.Offset-1])) || unicode.IsSpace(rune(r.value.Value[0]))
}

func (r *result) hasSpaceAfter(st *state) bool {
	return len(st.results) > 0 && r.value.Pos.Offset > 0 && r.value.Pos.Offset < len(st.file)-1 && unicode.IsSpace(rune(st.file[r.value.Pos.Offset+len(r.value.Value)]))
}

type state struct {
	file    string
	tokens  []lexer.Token
	results []result
	pos     int
}

func (s *state) addResult(group string, value lexer.Token) {
	s.results = append(s.results, result{
		pos:   s.pos,
		group: group,
		value: value,
	})
}

func (s *state) isNoMatch() bool {
	return s.pos == -1
}

func (s *state) isMatch() bool {
	return s.pos > -1
}

func (s state) noMatch() state {
	s.pos = -1
	return s
}

func (s *state) isEOF() bool {

	return len(s.tokens) == 0 || s.pos > len(s.tokens)-1 || s.pos > -1 && s.tokens[s.pos].Type == lexer.EOF
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
		st.addResult(a.group, orig.tokens[i])
	}
	return st
}

func capture(group string, s ...any) *combinator {
	comb := getCombinatorSlice(s...)
	return &combinator{
		parser: &asisCombinator{
			group:       group,
			combinators: comb,
		},
	}
}

// add c sequence "as-is"
func c(s ...any) *combinator {
	return capture("", s...)
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
	combinators []*combinator
}

func (n *notCombinator) Parse(st state) state {
	for _, comb := range n.combinators {
		newst := comb.ParseState(st)
		if newst.isMatch() {
			return st.noMatch()
		}
	}
	st.pos++
	return st
}

func not(s ...any) *combinator {
	return &combinator{
		parser: &notCombinator{
			combinators: getCombinatorSlice(s...),
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

func token(tok lexer.TokenType) *combinator {
	return &combinator{
		parser: &tokenCombinator{
			tok: tok,
		},
	}
}

func separated_by(separator any, s ...any) *combinator {
	var sequence = seq(s...)
	return seq(sequence, zero_or_more(separator, sequence))
}

type balancedAny struct {
	pairs []*combinator
}

func (b *balancedAny) Parse(st state) state {
	for i := 0; i < len(b.pairs); i += 2 {
		open := b.pairs[i]
		if open_match := open.ParseState(st); open_match.isMatch() {
			st := open_match
			balance := 1

			for !st.isEOF() {

				for j := 0; j < len(b.pairs); j += 2 {
					open := b.pairs[j]
					close := b.pairs[j+1]

					if open_match2 := open.ParseState(st); open_match2.isMatch() {
						balance++
						st.pos = open_match2.pos
					} else if close_match := close.ParseState(st); close_match.isMatch() {
						balance--
						st.pos = close_match.pos
					} else {
						st.pos++
					}

					if balance == 0 {
						return st
					}
				}
			}
		}
	}
	return st.noMatch()
}

func balanced_any(s ...any) *combinator {
	pairs := getCombinatorSlice(s...)
	if len(pairs)%2 != 0 {
		panic("balanced requires an even number of arguments")
	}

	return &combinator{
		parser: &balancedAny{
			pairs: pairs,
		},
	}
}

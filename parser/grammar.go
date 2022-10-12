package dmutparser

import (
	"io"

	"github.com/alecthomas/participle"
	lexer "github.com/alecthomas/participle/lexer"
	"github.com/alecthomas/participle/lexer/stateful"
)

type NeedFullMatch struct {
	Tokens []lexer.Token
}

type TopLevel struct {
	Decls    []MutationDecl `parser:"   (@@ |  "`
	Includes []Include      `parser:"  @@ )*"`
}

type Include struct {
	Path *string `parser:"   'include' @String ';'?   "`
}

type MutationReference struct {
	Name    string  `parser:"@SqlId"`
	Starred *string `parser:"@DotStar?"`
}

type MutationDecl struct {
	Name       *string              `parser:"   'mutation' @SqlId   "`
	DependsOn  *[]MutationReference `parser:"   ('depends' 'on' @@ (',' @@)* )?   "`
	Statements *[]ASTStatement      `parser:"   (@@)*   "`
}

type ASTStatement struct {
	// StartPos        lexer.Position
	NeedFullMatch
	UpOrDownStmt    *UpOrDownStmt    `parser:"   @@   "`
	CreateStatement *CreateStatement `parser:" | @@   "`
	RlsStatement    *RlsStatement    `parser:" | @@   "`
	GrantStatement  *GrantStatement  `parser:" | @@   "`
	// EndPos          lexer.Position
}

/// Auto grant
type GrantStatement struct {
	Perms *[]string `parser:"  'grant' @( !'on' )+ 'on'  "`
	Kind  *[]string `parser:"  @( 'table' | 'materialized'? 'view' | 'schema' | 'foreign' 'server' | 'tablespace' | 'foreign' 'data' 'wrapper' | 'database' | 'sequence' | 'function')?  "`
	Id    *string   `parser:"  @SqlId  "`
	To    *string   `parser:" 'to' @SqlId  "`
	Rest  *[]string `parser:"  ( @!';' )* @';'  "`
}

type UpOrDownStmt struct {
	Kind *string `parser:"   @('up' | 'down')   "`
	Stmt *string `parser:"   @MultilineString ';'   "`
}

/// A Create statement

type CreateStatement struct {
	Simple          *SimpleCreateStatement   `parser:" 'create' ('or' 'replace')? ( @@"`
	Function        *CreateFunctionStatement `parser:"  | @@   "`
	Index           *CreateIndexStatement    `parser:"  | @@   "`
	PolicyOrTrigger *CreatePolicyStmt        `parser:"  | @@ ) "`
	End             *string                  `parser:" @';'  "`
}

///

type RlsStatement struct {
	Table *string `parser:"  'alter' 'table' @SqlId 'enable' 'row' 'level' 'security' ';' "`
}

type CreateFunctionStatement struct {
	Name *string   `parser:"   'function' @SqlId '('   "`
	Args *[]string `parser:"   (@!')')* ')' (!(';'))+ "`
}

type CreateIndexStatement struct {
	Unique *string `parser:"  (@'unique')? 'index'"`
	Name   *string `parser:"  @SqlId 'on'  "`
	Table  *string `parser:" @SqlId  (!(';'))+  "`
}

type MultilineString struct {
	Start    *string `parser:"  @MultiStart   "`
	Contents *string `parser:"  (@Char)*  "`
	Stop     *string `parser:"  @MultiStop  "`
}

type SimpleCreateStatement struct {
	Kind *[]string `parser:"  @('table' | ('materialized')? 'view' | 'extension' | 'schema' | 'type' | 'role')"`
	Id   *string   `parser:"  @SqlId (!';')*   "`
}

type CreatePolicyStmt struct {
	Kind   *string   `parser:"   @('policy' | 'trigger')   "`
	Name   *string   `parser:"   @SqlId   "`
	Target *string   `parser:" (!'on')* 'on' @SqlId  "`
	Rest   *[]string `parser:" @!';'* "`
}

func r(name string, pattern string, actions ...stateful.Action) stateful.Rule {
	var act stateful.Action = nil
	if len(actions) > 0 {
		act = actions[0]
	}
	return stateful.Rule{Name: name, Pattern: pattern, Action: act}
}

// Parser is a dmut parser that outputs an AST
var (
	// The sql lexer
	// SqlLexer = stateful.Must(stateful.Rules{
	// 	"Root": {
	// 		r("MultiStart", `(\$[a-zA-Z_0-9]*\$)`, stateful.Push("MultilineString")),
	// 		stateful.Include("Common"),
	// 	},
	// 	"Common": {
	// 		r("whiteSpace", `(\s|\n)+|--[^\n]*\n?|/\*(.|\n)*?\*/`),
	// 		r("Semicolon", `;`),
	// 		r("SqlId", `(?:"(""|[^"])*"|[@$a-zA-Z_][\w$]*|\[[^\]]+\])(?:\.(?:"(""|[^"])*"|[@$a-zA-Z_][\w$]*|\[[^\]]+\]))*`),
	// 		r("String", `'(?:''|[^'])*'`),
	// 		r("Rest", `::|<>|!=|<=|>=|[-+?!~|^#*/%,.()=<>:\[\]]`),
	// 	},
	// 	"MultilineString": {
	// 		r("MultiStop", `\1`, stateful.Pop()),
	// 		r("Char", `(.|\n)`),
	// 	},
	// })

	Parser = participle.MustBuild(
		&TopLevel{},
		participle.UseLookahead(2),
		participle.Lexer(SqlLexer),
		participle.CaseInsensitive("SqlId"),
	)
)

func ParseString(filename string, str string) (*TopLevel, error) {
	var res = &TopLevel{}
	err := Parser.ParseString(filename, str, res)
	return res, err
}

func ParseReader(filename string, reader io.Reader) (*TopLevel, error) {
	var res = &TopLevel{}
	err := Parser.Parse(filename, reader, res)
	return res, err
}

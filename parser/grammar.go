package dmutparser

import (
	"io"

	"github.com/alecthomas/participle"
	lexer "github.com/alecthomas/participle/lexer"
)

type NeedFullMatch struct {
	Tok    lexer.Token
	EndTok lexer.Token
}

type TopLevel struct {
	Decls    *[]MutationDecl `parser:"   (@@    "`
	Includes *[]Include      `parser:"   | @@)*   "`
}

type Include struct {
	Path *string `parser:"   'include' @String ';'?   "`
}

type MutationDecl struct {
	Name       *string         `parser:"   'mutation' @SqlId   "`
	DependsOn  *[]string       `parser:"   ('depends' 'on' @SqlId (',' @SqlId)*)?   "`
	Statements *[]ASTStatement `parser:"   (@@)+   "`
}

type ASTStatement struct {
	// StartPos        lexer.Position
	NeedFullMatch
	UpOrDownStmt    *UpOrDownStmt    `parser:"   @@   "`
	CreateStatement *CreateStatement `parser:" | @@   "`
	GrantStatement  *GrantStatement  `parser:" | @@   "`
	// EndPos          lexer.Position
}

/// Auto grant
type GrantStatement struct {
	Perms *[]string `parser:"  'grant' ( @!'on' )+ 'on'  "`
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
	PolicyOrTrigger *CreatePolicyStmt        `parser:" | @@ ) "`
	End             *string                  `parser:" @';'  "`
}

///

type CreateFunctionStatement struct {
	Name *string   `parser:"   'function' @SqlId '('   "`
	Args *[]string `parser:"   (@!')')* ')' (!';')+  "`
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

// Parser is a dmut parser that outputs an AST
var Parser = participle.MustBuild(
	&TopLevel{},
	participle.UseLookahead(2),
	participle.Lexer(SqlLexer),
	participle.CaseInsensitive("SqlId"),
)

func ParseString(str string) (*TopLevel, error) {
	var res = &TopLevel{}
	err := Parser.ParseString(str, res)
	return res, err
}

func ParseReader(reader io.Reader) (*TopLevel, error) {
	var res = &TopLevel{}
	err := Parser.Parse(reader, res)
	return res, err
}

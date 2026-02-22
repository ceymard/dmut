package mutations

import (
	"fmt"
	"strings"

	"github.com/alecthomas/participle/v2"
	lexer "github.com/alecthomas/participle/v2/lexer"
)

type TopLevelStatement interface {
	Down() string
}

type AlterTableDownerStmt interface {
	Down() string
}

var (
	_ AlterTableDownerStmt = &AlterTableEnableRlsStmt{}
	_ AlterTableDownerStmt = &AlterTableAddColumnStmt{}
	_ AlterTableDownerStmt = &AlterTableAlterColumnSetDefaultStmt{}
	_ AlterTableDownerStmt = &AlterTableAddConstraintStmt{}
)

type DownerStatement interface {
	Down() string
}

var (
	_ DownerStatement = &CreateFunctionStatement{}
	_ DownerStatement = &CreateIndexStatement{}
	_ DownerStatement = &SimpleCreateStatement{}
	_ DownerStatement = &CreatePolicyOrTriggerStmt{}
	_ DownerStatement = &GrantStatement{}
	_ DownerStatement = &CommentStatement{}
)

type IMigrationStatement interface {
	Up() string
	Down() string
}

type NeedFullMatch struct {
	Tokens []lexer.Token
}

/** */
func (n *NeedFullMatch) Up() string {
	var str = ""
	for _, token := range n.Tokens {
		str += token.Value
	}
	return str
}

type CommentStatement struct {
	On   *[]string `parser:" @'comment' @'on' (!'is' @SqlId)+ "`
	Name *string   `parser:"  @SqlId  "`
	Rest *[]string `parser:" @!';'* ';'? "`
}

// Comments don't have an undo operation
func (comment *CommentStatement) Down() string {
	return ""
}

// / Auto grant
type GrantStatement struct {
	Perms *[]string `parser:"  'grant' @( !'on' )+ 'on'  "`
	Kind  *[]string `parser:"  @( 'table' | 'materialized'? 'view' | 'schema' | 'foreign' 'server' | 'tablespace' | 'foreign' 'data' 'wrapper' | 'database' | 'sequence' | 'function')?  "`
	Id    *string   `parser:"  @SqlId  "`
	To    *string   `parser:" 'to' @SqlId  "`
	Rest  *[]string `parser:"  ( @!';' )* @';'  "`
}

func (grant *GrantStatement) Down() string {
	var kind = ""
	if grant.Kind != nil {
		kind = strings.Join(*grant.Kind, " ")
	}
	return fmt.Sprintf(`REVOKE %s ON %s %s FROM %s;`, strings.Join(*grant.Perms, " "), kind, *grant.Id, *grant.To)
}

/// A Create statement

type CreateStatement struct {
	NeedFullMatch
	Statement DownerStatement `parser:" 'create' ('or' 'replace')? @@ ';'? "`
}

func (create *CreateStatement) Down() string {
	return create.Statement.Down()
}

// CREATE FUNCTION <name> <args> ... ;
type CreateFunctionStatement struct {
	Name *string   `parser:"   'function' @SqlId '('   "`
	Args *[]string `parser:"   (@!')')* ')' (!(';'))+ "`
}

func (fun *CreateFunctionStatement) Down() string {
	var args = ""
	if fun.Args != nil {
		args = strings.Join(*fun.Args, " ")
	}
	return fmt.Sprintf(`DROP FUNCTION %s (%s);`, *fun.Name, args)
}

// CREATE INDEX <name> ON <table> ...;
type CreateIndexStatement struct {
	Unique *string `parser:"  (@'unique')? 'index' ('concurrently')?"`
	Name   *string `parser:"  @SqlId 'on'  "`
	Table  *string `parser:" @SqlId  (!(';'))+  "`
}

func (ind *CreateIndexStatement) Down() string {
	var splt = strings.Split(*ind.Table, ".")
	var schema = ""
	if len(splt) > 1 {
		schema = splt[0] + "."
	}
	return fmt.Sprintf(`DROP INDEX %s%s;`, schema, *ind.Name)
}

// CREATE TABLE/VIEW/EXTENSION/SCHEMA/TYPE/ROLE <name> ... ;
type SimpleCreateStatement struct {
	Kind *[]string `parser:"  @('table' | ('materialized')? 'view' | 'extension' | 'schema' | 'type' | 'role')"`
	Id   *string   `parser:"  @SqlId (!';')*   "`
}

func (simple *SimpleCreateStatement) Down() string {
	return fmt.Sprintf(`DROP %s %s;`, strings.Join(*simple.Kind, " "), *simple.Id)
}

// CREATE POLICY <name> ON <table> ... ;
type CreatePolicyOrTriggerStmt struct {
	Kind   *string   `parser:"   @('policy' | 'trigger')   "`
	Name   *string   `parser:"   @SqlId   "`
	Target *string   `parser:" (!'on')* 'on' @SqlId  "`
	Rest   *[]string `parser:" @!';'* "`
}

func (pol *CreatePolicyOrTriggerStmt) Down() string {
	return fmt.Sprintf(`DROP %s %s ON %s;`, *pol.Kind, *pol.Name, *pol.Target)
}

//////

type AlterTableStmt struct {
	NeedFullMatch
	Table     *string              `parser:"  'alter' 'table'  @SqlId  "`
	Statement AlterTableDownerStmt `parser:" @@ "`
	Rest      *[]string            `parser:" @!';'* "`
	End       bool                 `parser:"  @';'?  "`
}

func (a *AlterTableStmt) Down() string {
	return fmt.Sprintf("ALTER TABLE %s %s;", *a.Table, a.Statement.Down())
}

var (
	_ AlterTableDownerStmt = &AlterTableEnableRlsStmt{}
)

// ALTER TABLE ENABLE ROW LEVEL SECURITY;
type AlterTableEnableRlsStmt struct {
	Table *string `parser:" 'enable' 'row' 'level' 'security' ';'? "`
}

func (a *AlterTableEnableRlsStmt) Down() string {
	return "DISABLE ROW LEVEL SECURITY"
}

// ALTER TABLE ADD COLUMN <name> <type>;
type AlterTableAddColumnStmt struct {
	Column *string   `parser:" 'add' 'column' @SqlId  "`
	Rest   *[]string `parser:" @!';'* ';'? "`
}

func (a *AlterTableAddColumnStmt) Down() string {
	return fmt.Sprintf("DROP COLUMN %s", *a.Column)
}

// ALTER TABLE ALTER COLUMN <name> SET DEFAULT <default>;
type AlterTableAlterColumnSetDefaultStmt struct {
	Column *string `parser:" 'alter' 'column' @SqlId 'set' 'default' (!';')* ';'? "`
}

func (a *AlterTableAlterColumnSetDefaultStmt) Down() string {
	return fmt.Sprintf("ALTER COLUMN %s DROP DEFAULT", *a.Column)
}

// ALTER TABLE ADD CONSTRAINT <name> ... ;
type AlterTableAddConstraintStmt struct {
	Constraint *string `parser:" 'add' 'constraint' @SqlId (!';')* ';'? "`
}

func (a *AlterTableAddConstraintStmt) Down() string {
	return fmt.Sprintf("DROP CONSTRAINT %s", *a.Constraint)
}

// ALTER TABLE RENAME COLUMN <name> TO <new name>;
type AlterTableRenameColumnStmt struct {
	Column   *string   `parser:" 'rename' 'column' @SqlId "`
	ToColumn *string   `parser:" 'to' @SqlId "`
	Rest     *[]string `parser:" @!';'* ';'? "`
}

func (a *AlterTableRenameColumnStmt) Down() string {
	return fmt.Sprintf("ALTER COLUMN %s RENAME TO %s", *a.ToColumn, *a.Column)
}

// ALTER TABLE RENAME CONSTRAINT <name> TO <new name>;
type AlterTableRenameConstraintStmt struct {
	Constraint   *string   `parser:" 'rename' 'constraint' @SqlId "`
	ToConstraint *string   `parser:" 'to' @SqlId "`
	Rest         *[]string `parser:" @!';'* ';'? "`
}

func (a *AlterTableRenameConstraintStmt) Down() string {
	return fmt.Sprintf("ALTER CONSTRAINT %s RENAME TO %s;", *a.ToConstraint, *a.Constraint)
}

// Parser is a dmut parser that outputs an AST
var (
	RestPattern = `\d+|::|<>|!=|<=|>=|[-+?!~|^#*/%,.()=<>:\[\]]`
	// The sql lexer
	SqlLexer = lexer.MustStateful(lexer.Rules{
		"Root": {
			{Name: "MultiStart", Pattern: `(\$[a-zA-Z_0-9]*\$)`, Action: lexer.Push("MultilineString")},
			{Name: "whiteSpace", Pattern: `(\s|\n)+|--[^\n]*\n?|/\*(.|\n)*?\*/`},
			{Name: "Semicolon", Pattern: `;`},
			{Name: "SqlId", Pattern: `(?:"(""|[^"])*"|[@$a-zA-Z_][\w$]*|\[[^\]]+\])(?:\.(?:"(""|[^"])*"|[@$a-zA-Z_][\w$]*|\[[^\]]+\]))*`},
			{Name: "String", Pattern: `'(?:''|[^'])*'`},
			{Name: "Rest", Pattern: RestPattern},
		},
		"Common": {
			{Name: "whiteSpace", Pattern: `(\s|\n)+|--[^\n]*\n?|/\*(.|\n)*?\*/`},
			{Name: "Semicolon", Pattern: `;`},
			{Name: "SqlId", Pattern: `(?:"(""|[^"])*"|[@$a-zA-Z_][\w$]*|\[[^\]]+\])(?:\.(?:"(""|[^"])*"|[@$a-zA-Z_][\w$]*|\[[^\]]+\]))*`},
			{Name: "String", Pattern: `'(?:''|[^'])*'`},
			{Name: "Rest", Pattern: RestPattern},
		},
		"MultilineString": {
			{Name: "MultiStop", Pattern: `\1`, Action: lexer.Pop()},
			{Name: "Char", Pattern: `(.|\n)`},
		},
	})

	// SqlLexer is a lexer for sql
	// SqlLexer = Regexp2Lexer().Token(
	// 	"_WhiteSpace", `\s+|--[^\n]*\n?|/\*(.|\n)*?\*/`,
	// ).Token(
	// 	"Semicolon", `;`,
	// ).Token(
	// 	"MultilineString", `\$[a-zA-Z_0-9]*\$`,
	// ).EndMatch(`\1`).Token(
	// 	"SqlId", `(?:"(""|[^"])*"|[@$a-zA-Z_][\w$]*|\[[^\]]+\])(?:\.(?:"(""|[^"])*"|[@$a-zA-Z_][\w$]*|\[[^\]]+\]))*`,
	// ).Token(
	// 	"DotStar", `\.\*`,
	// ).Token(
	// 	"Number", `[-+]?\d*\.?\d+([eE][-+]?\d+)?`,
	// ).Token(
	// 	"String", `'(?:''|[^'])*'`,
	// ).Token(
	// 	"Rest", `::|<>|!=|<=|>=|[-+?!~|^#*/%,.(&)=<>:\[\]]`,
	// )

	Parser = participle.MustBuild[TopLevelStatement](
		participle.UseLookahead(2),
		participle.Lexer(SqlLexer),
		participle.CaseInsensitive("SqlId"),
		participle.Elide("whiteSpace"),
		participle.Union[AlterTableDownerStmt](&AlterTableEnableRlsStmt{}, &AlterTableAddColumnStmt{}, &AlterTableAlterColumnSetDefaultStmt{}, &AlterTableAddConstraintStmt{}),
		participle.Union[DownerStatement](&CreateFunctionStatement{}, &CreateIndexStatement{}, &SimpleCreateStatement{}, &CreatePolicyOrTriggerStmt{}, &CommentStatement{}),
		participle.Union[TopLevelStatement](&AlterTableStmt{}, &CreateStatement{}, &GrantStatement{}, &CommentStatement{}),
	)
)

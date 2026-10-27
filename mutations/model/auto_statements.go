package model

import (
	"strings"

	lexer "github.com/alecthomas/participle/v2/lexer"
)

// Parser overview: recursive descent that builds the "opposite" statement (e.g. CREATE → DROP).
// Syntax validation is left to the database; this parser does not report SQL errors.
//
// Accumulation: c(rule) and capture(group, rule) collect tokens in match order.
// c(rule) is shorthand for capture("", rule), i.e. the default (unnamed) group.
//
// .Produce() replaces matched tokens with literal strings or captured groups.
// Produce writes into the default group and overwrites what was captured for the rule it runs on.
// Do not call .Produce() inside capture() or c(); behavior is undefined.
//
// Use the default group when order is fine; use named groups when token cannot be captured in order.

var id = token(SqlLexer.Symbols()["Id"])
var id2 = token(SqlLexer.Symbols()["Id"])
var operator = token(SqlLexer.Symbols()["Operator"])
var captured = group("")
var if_not_exists = opt("if", "not", "exists")

// var balanced_expr = c("balanced_expr")

// Doc comments (--? and /*?) are tokens of their own. The rules below let them through only
// where one documents something, and capture them into groups that Produce keeps through keepDocs:
//
//	doc         a doc comment
//	spec        the tokens that name the created object, as in its DROP
//	column      a column of a column list
//	constraint  a named constraint
//	unnamed     a constraint with no name, which can't be documented
//	tbl, col    the table and the column of ALTER TABLE ... ADD COLUMN
//
// docs reads the doc comments that precede a statement or an item.
var docs = capture("doc", zero_or_more(token(tokDoc)))

// column_item is one entry of a column list: its doc comments, then a column, a named constraint
// or an unnamed one, then whatever follows up to the next comma.
var column_item = seq(
	docs,
	either(
		seq("constraint", capture("constraint", id)),
		capture("unnamed", either("primary", "unique", "check", "foreign", "exclude", "like")),
		capture("column", id),
	),
	zero_or_more(either(
		// A named constraint in the middle of a column definition.
		seq(capture("doc", token(tokDoc), zero_or_more(token(tokDoc))), "constraint", capture("constraint", id)),
		balanced_any("(", ")"),
		not(",", ")"),
	)),
)

// column_list is the list of columns of a table, a composite type or a view, in which every
// column and every named constraint may have doc comments before it.
var column_list = seq("(", column_item, zero_or_more(",", column_item), ")")

var auto_create = seq("create",
	opt("or", "replace"),
	either(

		// OPERATOR
		// https://www.postgresql.org/docs/18/sql-createoperator.html
		// https://www.postgresql.org/docs/18/sql-dropoperator.html
		seq(
			c("operator"),
			c(opt(id, ".")),
			c(operator),
			if_not_exists,
			"(",
			zero_or_more(either(
				seq("leftarg", "=", capture("left", id)),
				seq("rightarg", "=", capture("right", id)),
				zero_or_more(not(",", ")")),
			), opt(",")),
			")",
		).Produce(captured, "(", groupOrDefault("left", "none"), ",", group("right"), ")"),

		// OPERATOR CLASS
		// https://www.postgresql.org/docs/18/sql-createopclass.html
		// https://www.postgresql.org/docs/18/sql-dropopclass.html
		seq(
			c("operator", "class", id),
			opt("default"),
			"for", "type", id,
			c("using", id),
		),

		// OPERATOR FAMILY
		// https://www.postgresql.org/docs/18/sql-createopfamily.html
		// https://www.postgresql.org/docs/18/sql-dropopfamily.html
		seq(
			c("operator", "family", id),
			c("using", id),
		),

		// AGGREGATE, FUNCTION, PROCEDURE
		// https://www.postgresql.org/docs/18/sql-createfunction.html
		// https://www.postgresql.org/docs/18/sql-dropfunction.html
		// https://www.postgresql.org/docs/18/sql-createaggregate.html
		// https://www.postgresql.org/docs/18/sql-dropaggregate.html
		// https://www.postgresql.org/docs/18/sql-createprocedure.html
		// https://www.postgresql.org/docs/18/sql-dropprocedure.html
		seq(
			c(either("function", "aggregate", "procedure"), id, "("),
			zero_or_more(seq(
				capture("mode", opt(either("in", "inout", "out", "variadic"))),
				capture("pname", id),
				capture("ptype", opt(zero_or_more(not("default", "=", ",", ")")))),
				opt(either("=", "default"), zero_or_more(either(balanced_any("(", ")"), not(")", ",")))),
				// until(either(",", ")")),
				opt(","),
			).Produce(produceFunc(func(st state, old_results []result) state {
				// Postgres identifies a function/procedure to DROP by its IN,
				// INOUT and VARIADIC parameter types only - an OUT parameter
				// isn't part of that signature, so it must be left out of the
				// generated DROP argument list.
				mode := ""
				for _, r := range old_results {
					if r.group == "mode" {
						mode = strings.ToLower(r.value.Value)
					}
				}
				if mode == "out" {
					return st
				}
				hasPrev := false
				for _, r := range st.results {
					if r.group == "param_sep" {
						hasPrev = true
						break
					}
				}
				if hasPrev {
					st.addResult("", lexer.Token{Value: ",", Pos: lexer.Position{Offset: -1}})
				}
				for _, r := range old_results {
					if r.group == "pname" || r.group == "ptype" {
						st.addResult("", r.value)
					}
				}
				st.addResult("param_sep", lexer.Token{Value: "", Pos: lexer.Position{Offset: -1}})
				return st
			}))),
			c(")"),
		),

		// ACCESS METHOD
		// https://www.postgresql.org/docs/18/sql-create-access-method.html
		// https://www.postgresql.org/docs/18/sql-drop-access-method.html
		c("access", "method", id),

		// FOREIGN DATA WRAPPER
		// https://www.postgresql.org/docs/18/sql-createforeigndatawrapper.html
		// https://www.postgresql.org/docs/18/sql-dropforeigndatawrapper.html
		c("foreign", "data", "wrapper", id),

		// FOREIGN TABLE
		// https://www.postgresql.org/docs/18/sql-createtable.html
		// https://www.postgresql.org/docs/18/sql-droptable.html
		seq(c("foreign", "table"), if_not_exists, c(id), opt(column_list)),

		// POLICY
		// https://www.postgresql.org/docs/18/sql-createpolicy.html
		// https://www.postgresql.org/docs/18/sql-droppolicy.html
		seq(
			c("policy", id),
			c("on", id),
		),

		// CAST
		// https://www.postgresql.org/docs/18/sql-createcast.html
		// https://www.postgresql.org/docs/18/sql-dropcast.html
		c("cast", "(", id, "as", id, ")"),

		// EVENT TRIGGER
		// https://www.postgresql.org/docs/18/sql-createeventtrigger.html
		// https://www.postgresql.org/docs/18/sql-dropeventtrigger.html
		c("event", "trigger", id),

		// INDEX
		// https://www.postgresql.org/docs/18/sql-createindex.html
		// https://www.postgresql.org/docs/18/sql-dropindex.html
		seq(opt("unique"), c("index"), opt("concurrently"), if_not_exists,
			seq(
				capture("index_name", id),
				"on",
				capture("schema", id).Produce(produceFunc(func(st state, old_results []result) state {
					var _id = old_results[0].value.Value
					if strings.Contains(_id, ".") {
						// return the schema name
						// return strings.Split(_id, ".")[0]
						st.addResult("schema", lexer.Token{Value: strings.Split(_id, ".")[0], Pos: lexer.Position{Offset: -1}})
						st.addResult("schema", lexer.Token{Value: ".", Pos: lexer.Position{Offset: -1}})
						return st
					}
					st.addResult("schema", lexer.Token{Value: "", Pos: lexer.Position{Offset: -1}})
					return st
				})),
			).Produce(group("schema"), group("index_name")),
		),
		// LANGUAGE
		// https://www.postgresql.org/docs/18/sql-createlanguage.html
		// https://www.postgresql.org/docs/18/sql-droplanguage.html
		seq(opt("trusted"), c(opt("procedural"), "language", id)),

		// COLLATION
		// https://www.postgresql.org/docs/18/sql-createcollation.html
		// https://www.postgresql.org/docs/18/sql-dropcollation.html
		seq(c("collation"), if_not_exists, c(id)),

		// CONVERSION
		// https://www.postgresql.org/docs/18/sql-createconversion.html
		// https://www.postgresql.org/docs/18/sql-dropconversion.html
		seq(opt("default"), c("conversion", id), "for", id),

		// CONSTRAINT
		// https://www.postgresql.org/docs/18/sql-createconstraint.html
		// https://www.postgresql.org/docs/18/sql-dropconstraint.html
		seq(opt("constraint"),
			c("trigger", id),
			either("before", "after", seq("instead", "of")),
			separated_by("or", either(
				"insert",
				seq("update", opt("of", id, zero_or_more(",", id))),
				"delete",
				"truncate",
			)),
			c("on", id),
		),

		// SEQUENCE
		// https://www.postgresql.org/docs/18/sql-createsequence.html
		// https://www.postgresql.org/docs/18/sql-dropsequence.html
		seq(zero_or_more(either("unlogged", "temporary", "temp")), c("sequence"), if_not_exists, c(id)),

		// RULE
		// https://www.postgresql.org/docs/18/sql-createrule.html
		// https://www.postgresql.org/docs/18/sql-droprule.html
		seq(c("rule", id), "as", c("on"), id, "to", c(id)),

		// TYPE
		// https://www.postgresql.org/docs/18/sql-createtype.html
		// https://www.postgresql.org/docs/18/sql-droptype.html
		seq(c("type", id), opt("as", column_list)),

		// SCHEMA
		// https://www.postgresql.org/docs/18/sql-createschema.html
		// https://www.postgresql.org/docs/18/sql-dropschema.html
		seq(c("schema"), if_not_exists, c(id)),

		// TABLE
		// https://www.postgresql.org/docs/18/sql-createtable.html
		// https://www.postgresql.org/docs/18/sql-droptable.html
		seq(
			opt(either("global", "local")),
			opt(either("temporary", "temp")),
			opt(either("unlogged")),
			c("table"),
			if_not_exists,
			c(id),
			opt(column_list),
		),

		// TABLESPACE
		// https://www.postgresql.org/docs/18/sql-createtablespace.html
		// https://www.postgresql.org/docs/18/sql-droptablespace.html
		c("tablespace", id),

		// TEXT SEARCH CONFIGURATION / DICTIONARY / PARSER / TEMPLATE
		// https://www.postgresql.org/docs/18/sql-createtsconfig.html
		// https://www.postgresql.org/docs/18/sql-droptsconfig.html
		// https://www.postgresql.org/docs/18/sql-createtsdictionary.html
		// https://www.postgresql.org/docs/18/sql-droptsdictionary.html
		// https://www.postgresql.org/docs/18/sql-createtsparser.html
		// https://www.postgresql.org/docs/18/sql-droptsparser.html
		// https://www.postgresql.org/docs/18/sql-createtstemplate.html
		// https://www.postgresql.org/docs/18/sql-droptstemplate.html
		c("text", "search", either("configuration", "dictionary", "parser", "template"), id),

		// TRANSFORM
		// https://www.postgresql.org/docs/18/sql-createtransform.html
		// https://www.postgresql.org/docs/18/sql-droptransform.html
		c("transform", "for", id, "language", id),

		// ROLE
		// https://www.postgresql.org/docs/18/sql-createrole.html
		// https://www.postgresql.org/docs/18/sql-droprole.html
		c("role", id),

		// MATERIALIZED VIEW / VIEW
		// https://www.postgresql.org/docs/18/sql-createview.html
		// https://www.postgresql.org/docs/18/sql-dropview.html
		seq(c("materialized", "view"), if_not_exists, c(id), opt(column_list)),
		seq(
			opt(either("temp", "temporary")),
			opt("recursive"),
			c("view", id),
			opt(column_list),
		),

		// EXTENSION
		// https://www.postgresql.org/docs/18/sql-createextension.html
		// https://www.postgresql.org/docs/18/sql-dropextension.html
		seq(c("extension"), if_not_exists, c(id)),

		// DOMAIN
		// https://www.postgresql.org/docs/18/sql-createdomain.html
		// https://www.postgresql.org/docs/18/sql-dropdomain.html
		c("domain", id),

		// PUBLICATION
		// https://www.postgresql.org/docs/18/sql-createpublication.html
		// https://www.postgresql.org/docs/18/sql-droppublication.html
		c("publication", id),

		// SERVER
		// https://www.postgresql.org/docs/18/sql-createserver.html
		// https://www.postgresql.org/docs/18/sql-dropserver.html
		seq(c("server"), if_not_exists, c(id)),

		// STATISTICS
		// https://www.postgresql.org/docs/18/sql-createstatistics.html
		// https://www.postgresql.org/docs/18/sql-dropstatistics.html
		seq(c("statistics"), if_not_exists, c(id)),

		// SUBSCRIPTION
		// https://www.postgresql.org/docs/18/sql-createsubscription.html
		// https://www.postgresql.org/docs/18/sql-dropsubscription.html
		c("subscription", id),

		// USER MAPPING
		// https://www.postgresql.org/docs/18/sql-createusermapping.html
		// https://www.postgresql.org/docs/18/sql-dropusermapping.html
		c("user", "mapping", "for", id, "server", id),
	),
	// Get everything until a terminating ;

).Produce("DROP", captured, ";", specOf, keepDocs)

// COMMENT ON has no reverse : we leave the comment in place rather than guess what
// it used to be, so it can be written as a plain statement with no `down`.
var auto_comment = seq("comment", "on", until_opt(";")).Produce(noDown())

var auto_alter_table = seq(c("alter", "table", capture("tbl", id)),
	either(
		seq(str("inherit").Produce("no inherit"), c(id)),
		seq("enable", c("row", "level", "security")).Produce("disable", captured),
		// The word COLUMN is optional, unless the name could start a constraint.
		seq("add", either(
			seq("column", if_not_exists, c(capture("col", id))),
			c(capture("col", id_except("constraint", "primary", "unique", "check", "foreign", "exclude"))),
		)).Produce("drop column", captured, keepDocs),
		seq(c("alter", "column", id), str("set").Produce("drop"), c("default")),
		seq("add", c("constraint", id)).Produce("drop", captured),
		seq(c("rename", either("constraint", "column")), capture("from", id), "to", capture("to", id)).Produce(captured, group("to"), "to", group("from")),
		// ATTACH/DETACH PARTITION is a lossless, symmetric pair, but only in
		// the attach direction: attaching just needs the partition name to
		// undo (detach), while detaching would need the original bounds
		// (FOR VALUES ...) to reattach, and those aren't present in the
		// DETACH statement itself — so DETACH intentionally has no auto-down
		// here and falls through to requiring a manual down.
		seq("attach", c("partition", id)).Produce("detach", captured),
	),
).Produce(captured, ";", keepDocs)

var auto_grant = seq(
	"grant",
	either(
		seq(c(id), "to", capture("to", separated_by(",", id2))),
		seq(c("all", "tables", "in", "schema", id), "to", capture("to", id2)),
		seq(
			c(until("on"), "on"),
			opt(c(either(
				seq("all", either("tables", "sequences", "functions", "routines", "procedures"), "in", "schema"),
				"table",
				seq(opt("materialized"), "view"),
				"schema",
				"foreign", "server",
				"foreign", "data", "wrapper",
				"tablespace",
				"database",
				"sequence",
				"function",
				"language",
				"parameter",
				seq("large", "object"),
				"type",
				"tablespace",
			))),
			c(id),
			// Functions/procedures carry an argument list: `on function f(int, text)`.
			opt(c(balanced_any("(", ")"))),
			"to",
			capture("to", id, zero_or_more(",", id)),
		),
	),
).Produce("revoke", captured, " from", group("to"), ";")

var default_privileges = seq(
	c("alter", "default", "privileges", "in", "schema", id),
	str("grant").Produce("revoke"),
	c(until("on"), "on"),
	c(until("to")),
	str("to").Produce("from"),
	c(id),
	c(opt(";")),
)

var AutoDowner = seq(docs, either(default_privileges, auto_create, auto_alter_table, auto_grant, auto_comment), until_opt(";"))

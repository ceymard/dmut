package mutations

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

// var balanced_expr = c("balanced_expr")

var auto_create = seq("create",
	either(

		// OPERATOR
		// https://www.postgresql.org/docs/18/sql-createoperator.html
		// https://www.postgresql.org/docs/18/sql-dropoperator.html
		seq(
			c("operator"),
			c(opt(id, ".")),
			c(operator),
			"(",
			zero_or_more(not(")"), either(
				seq("leftarg", "=", capture("left", id)),
				seq("rightarg", "=", capture("right", id)),
				until(either(",", ")")),
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
				opt(either("in", "inout", "out", "variadic")),
				c(id),
				opt(seq(not("default"), c(id))),
				opt(either("=", "default")),
				until(either(",", ")")),
				c(opt(",")),
			)),
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
		c("foreign", "table", id),

		// POLICY
		// https://www.postgresql.org/docs/18/sql-createpolicy.html
		// https://www.postgresql.org/docs/18/sql-droppolicy.html
		c("policy", id, "on", id),

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
		seq(opt("unique"), c("index"), opt("concurrently"), c(id)),

		// LANGUAGE
		// https://www.postgresql.org/docs/18/sql-createlanguage.html
		// https://www.postgresql.org/docs/18/sql-droplanguage.html
		seq(opt("trusted"), c(opt("procedural"), "language", id)),

		// COLLATION
		// https://www.postgresql.org/docs/18/sql-createcollation.html
		// https://www.postgresql.org/docs/18/sql-dropcollation.html
		c("collation", id),

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
				seq("update", opt("of", id, zero_or_more(",", c(id)))),
				"delete",
				"truncate",
			)),
			c("on", id),
		),

		// SEQUENCE
		// https://www.postgresql.org/docs/18/sql-createsequence.html
		// https://www.postgresql.org/docs/18/sql-dropsequence.html
		seq(zero_or_more(either("unlogged", "temporary", "temp")), c("sequence", id)),

		// RULE
		// https://www.postgresql.org/docs/18/sql-createrule.html
		// https://www.postgresql.org/docs/18/sql-droprule.html
		seq(c("rule", id), "as", c("on"), id, "to", c(id)),

		// TYPE
		// https://www.postgresql.org/docs/18/sql-createtype.html
		// https://www.postgresql.org/docs/18/sql-droptype.html
		c("type", id),

		// SCHEMA
		// https://www.postgresql.org/docs/18/sql-createschema.html
		// https://www.postgresql.org/docs/18/sql-dropschema.html
		c("schema", id),

		// TABLE
		// https://www.postgresql.org/docs/18/sql-createtable.html
		// https://www.postgresql.org/docs/18/sql-droptable.html
		seq(
			opt(either("global", "local")),
			opt(either("temporary", "temp")),
			opt(either("unlogged")),
			c("table", id),
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
		c("materialized", "view", id),
		seq(
			opt(either("temp", "temporary")),
			opt("recursive"),
			c("view", id),
		),

		// EXTENSION
		// https://www.postgresql.org/docs/18/sql-createextension.html
		// https://www.postgresql.org/docs/18/sql-dropextension.html
		c("extension", id),

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
		c("server", id),

		// STATISTICS
		// https://www.postgresql.org/docs/18/sql-createstatistics.html
		// https://www.postgresql.org/docs/18/sql-dropstatistics.html
		c("statistics", id),

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

).Produce("DROP", captured, ";")

var auto_comment = seq("comment", "on", until_opt(";")).Produce("")

var auto_alter_table = seq(c("alter", "table", id),
	either(
		seq(str("inherit").Produce("no inherit"), c(id)),
		seq("enable", c("row", "level", "security")).Produce("disable", captured),
		seq("add", c("column", id)).Produce("drop", captured),
		seq(c("alter", "column", id), str("set").Produce("drop"), c("default")),
		seq("add", c("constraint", id)).Produce("drop", captured),
		seq(c("rename", either("constraint", "column")), capture("from", id), "to", capture("to", id)).Produce(captured, group("to"), "to", group("from")),
	),
).Produce(captured, ";")

var auto_grant = seq(
	"grant",
	either(
		seq(c(id), "to", capture("to", separated_by(",", id2))),
		seq(
			c(until("on"), "on"),
			opt(c(either(
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
			"to",
			capture("to", id, zero_or_more(",", id)),
		),
	),
).Produce("revoke", captured, " from", group("to"), ";")

var AutoDowner = seq(either(auto_create, auto_alter_table, auto_grant, auto_comment), until_opt(";"))

package mutations

// Parser overview: recursive descent that builds the "opposite" statement (e.g. CREATE → DROP).
// Syntax validation is left to the database; this parser does not report SQL errors.
//
// Accumulation: a(rule) and accumulate(group, rule) collect tokens in match order.
// a(rule) is shorthand for accumulate("", rule), i.e. the default (unnamed) group.
//
// .Produce() replaces matched tokens with literal strings or accumulated groups.
// Produce writes into the default group and overwrites what was accumulated for the rule it runs on.
// Do not call .Produce() inside accumulate() or a(); behavior is undefined.
//
// Use the default group when order is fine; use named groups when token cannot be collected in order.

var id = token(SqlLexer.Symbols()["Id"])
var id2 = token(SqlLexer.Symbols()["Id"])
var operator = token(SqlLexer.Symbols()["Operator"])
var accumulated = group("")
var balanced_expr = a("balanced_expr")

var auto_create = seq("create",
	either(

		// OPERATOR
		// https://www.postgresql.org/docs/18/sql-createoperator.html
		// https://www.postgresql.org/docs/18/sql-dropoperator.html
		seq(
			a("operator"),
			a(opt(id, ".")),
			a(operator),
			"(",
			zero_or_more(not(")"), either(
				seq("leftarg", "=", accumulate("left", id)),
				seq("rightarg", "=", accumulate("right", id)),
				until(either(",", ")")),
			), opt(",")),
			")",
		).Produce(accumulated, "(", groupDef("left", "none"), ",", group("right"), ")"),

		// OPERATOR CLASS
		// https://www.postgresql.org/docs/18/sql-createopclass.html
		// https://www.postgresql.org/docs/18/sql-dropopclass.html
		seq(
			a("operator", "class", id),
			opt("default"),
			"for", "type", id,
			a("using", id),
		),

		// OPERATOR FAMILY
		// https://www.postgresql.org/docs/18/sql-createopfamily.html
		// https://www.postgresql.org/docs/18/sql-dropopfamily.html
		seq(
			a("operator", "family", id),
			a("using", id),
		),

		// AGGREGATE, FUNCTION, PROCEDURE
		// https://www.postgresql.org/docs/18/sql-createfunction.html
		// https://www.postgresql.org/docs/18/sql-dropfunction.html
		// https://www.postgresql.org/docs/18/sql-createaggregate.html
		// https://www.postgresql.org/docs/18/sql-dropaggregate.html
		// https://www.postgresql.org/docs/18/sql-createprocedure.html
		// https://www.postgresql.org/docs/18/sql-dropprocedure.html
		seq(
			a(either("function", "aggregate", "procedure"), id, "("),
			zero_or_more(seq(
				opt(either("in", "inout", "out", "variadic")),
				a(id),
				opt(seq(not("default"), a(id))),
				opt(either("=", "default")),
				until(either(",", ")")),
				a(opt(",")),
			)),
			a(")"),
		),

		// ACCESS METHOD
		// https://www.postgresql.org/docs/18/sql-create-access-method.html
		// https://www.postgresql.org/docs/18/sql-drop-access-method.html
		a("access", "method", id),

		// FOREIGN DATA WRAPPER
		// https://www.postgresql.org/docs/18/sql-createforeigndatawrapper.html
		// https://www.postgresql.org/docs/18/sql-dropforeigndatawrapper.html
		a("foreign", "data", "wrapper", id),

		// FOREIGN TABLE
		// https://www.postgresql.org/docs/18/sql-createtable.html
		// https://www.postgresql.org/docs/18/sql-droptable.html
		a("foreign", "table", id),

		// POLICY
		// https://www.postgresql.org/docs/18/sql-createpolicy.html
		// https://www.postgresql.org/docs/18/sql-droppolicy.html
		a("policy", id, "on", id),

		// CAST
		// https://www.postgresql.org/docs/18/sql-createcast.html
		// https://www.postgresql.org/docs/18/sql-dropcast.html
		a("cast", "(", id, "as", id, ")"),

		// EVENT TRIGGER
		// https://www.postgresql.org/docs/18/sql-createeventtrigger.html
		// https://www.postgresql.org/docs/18/sql-dropeventtrigger.html
		a("event", "trigger", id),

		// INDEX
		// https://www.postgresql.org/docs/18/sql-createindex.html
		// https://www.postgresql.org/docs/18/sql-dropindex.html
		seq(opt("unique"), a("index"), opt("concurrently"), a(id)),

		// LANGUAGE
		// https://www.postgresql.org/docs/18/sql-createlanguage.html
		// https://www.postgresql.org/docs/18/sql-droplanguage.html
		seq(opt("trusted"), a(opt("procedural"), "language", id)),

		// COLLATION
		// https://www.postgresql.org/docs/18/sql-createcollation.html
		// https://www.postgresql.org/docs/18/sql-dropcollation.html
		a("collation", id),

		// CONVERSION
		// https://www.postgresql.org/docs/18/sql-createconversion.html
		// https://www.postgresql.org/docs/18/sql-dropconversion.html
		seq(opt("default"), a("conversion", id), "for", id),

		// CONSTRAINT
		// https://www.postgresql.org/docs/18/sql-createconstraint.html
		// https://www.postgresql.org/docs/18/sql-dropconstraint.html
		seq(opt("constraint"),
			a("trigger", id),
			either("before", "after", seq("instead", "of")),
			separated_by("or", either(
				"insert",
				seq("update", opt("of", id, zero_or_more(",", a(id)))),
				"delete",
				"truncate",
			)),
			a("on", id),
		),

		// SEQUENCE
		// https://www.postgresql.org/docs/18/sql-createsequence.html
		// https://www.postgresql.org/docs/18/sql-dropsequence.html
		seq(zero_or_more(either("unlogged", "temporary", "temp")), "sequence", id),

		// RULE
		// https://www.postgresql.org/docs/18/sql-createrule.html
		// https://www.postgresql.org/docs/18/sql-droprule.html
		seq(a("rule", id), "as", a("on"), id, "to", a(id)),

		// TYPE
		// https://www.postgresql.org/docs/18/sql-createtype.html
		// https://www.postgresql.org/docs/18/sql-droptype.html
		a("type", id),

		// SCHEMA
		// https://www.postgresql.org/docs/18/sql-createschema.html
		// https://www.postgresql.org/docs/18/sql-dropschema.html
		a("schema", id),

		// TABLE
		// https://www.postgresql.org/docs/18/sql-createtable.html
		// https://www.postgresql.org/docs/18/sql-droptable.html
		seq(
			opt(either("global", "local")),
			opt(either("temporary", "temp")),
			opt(either("unlogged")),
			a("table", id),
		),

		// TABLESPACE
		// https://www.postgresql.org/docs/18/sql-createtablespace.html
		// https://www.postgresql.org/docs/18/sql-droptablespace.html
		a("tablespace", id),

		// TEXT SEARCH CONFIGURATION / DICTIONARY / PARSER / TEMPLATE
		// https://www.postgresql.org/docs/18/sql-createtsconfig.html
		// https://www.postgresql.org/docs/18/sql-droptsconfig.html
		// https://www.postgresql.org/docs/18/sql-createtsdictionary.html
		// https://www.postgresql.org/docs/18/sql-droptsdictionary.html
		// https://www.postgresql.org/docs/18/sql-createtsparser.html
		// https://www.postgresql.org/docs/18/sql-droptsparser.html
		// https://www.postgresql.org/docs/18/sql-createtstemplate.html
		// https://www.postgresql.org/docs/18/sql-droptstemplate.html
		a("text", "search", either("configuration", "dictionary", "parser", "template"), id),

		// TRANSFORM
		// https://www.postgresql.org/docs/18/sql-createtransform.html
		// https://www.postgresql.org/docs/18/sql-droptransform.html
		a("transform", "for", id, "language", id),

		// ROLE
		// https://www.postgresql.org/docs/18/sql-createrole.html
		// https://www.postgresql.org/docs/18/sql-droprole.html
		a("role", id),

		// MATERIALIZED VIEW / VIEW
		// https://www.postgresql.org/docs/18/sql-createview.html
		// https://www.postgresql.org/docs/18/sql-dropview.html
		a("materialized", "view", id),
		seq(
			opt(either("temp", "temporary")),
			opt("recursive"),
			a("view", id),
		),

		// EXTENSION
		// https://www.postgresql.org/docs/18/sql-createextension.html
		// https://www.postgresql.org/docs/18/sql-dropextension.html
		a("extension", id),

		// DOMAIN
		// https://www.postgresql.org/docs/18/sql-createdomain.html
		// https://www.postgresql.org/docs/18/sql-dropdomain.html
		a("domain", id),

		// PUBLICATION
		// https://www.postgresql.org/docs/18/sql-createpublication.html
		// https://www.postgresql.org/docs/18/sql-droppublication.html
		a("publication", id),

		// SERVER
		// https://www.postgresql.org/docs/18/sql-createserver.html
		// https://www.postgresql.org/docs/18/sql-dropserver.html
		a("server", id),

		// STATISTICS
		// https://www.postgresql.org/docs/18/sql-createstatistics.html
		// https://www.postgresql.org/docs/18/sql-dropstatistics.html
		a("statistics", id),

		// SUBSCRIPTION
		// https://www.postgresql.org/docs/18/sql-createsubscription.html
		// https://www.postgresql.org/docs/18/sql-dropsubscription.html
		a("subscription", id),

		// USER MAPPING
		// https://www.postgresql.org/docs/18/sql-createusermapping.html
		// https://www.postgresql.org/docs/18/sql-dropusermapping.html
		a("user", "mapping", "for", id, "server", id),
	),
	// Get everything until a terminating ;

).Produce("DROP", accumulated, ";")

var auto_comment = seq("comment", "on", until_opt(";")).Produce("")

var auto_alter_table = seq(a("alter", "table", id),
	either(
		seq(str("inherit").Produce("no inherit"), a(id)),
		seq("enable", a("row", "level", "security")).Produce("disable", accumulated),
		seq("add", a("column", id)).Produce("drop", accumulated),
		seq(a("alter", "column", id), str("set").Produce("drop"), a("default")),
		seq("add", a("constraint", id)).Produce("drop", accumulated),
		seq(a("rename", either("constraint", "column")), accumulate("from", id), "to", accumulate("to", id)).Produce(accumulated, group("to"), "to", group("from")),
	),
).Produce(accumulated, ";")

var auto_grant = seq(
	"grant",
	either(
		seq(a(id), "to", accumulate("to", separated_by(",", id2))),
		seq(
			a(until("on"), "on"),
			opt(a(either(
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
			a(id),
			"to",
			accumulate("to", id, zero_or_more(",", id)),
		),
	),
).Produce("revoke", accumulated, " from", group("to"), ";")

var AutoDowner = seq(either(auto_create, auto_alter_table, auto_grant, auto_comment), until_opt(";"))

package mutations

var id = token("", SqlLexer.Symbols()["Id"])
var id2 = token("", SqlLexer.Symbols()["Id"])
var operator = token("", SqlLexer.Symbols()["Operator"])
var acc = group("")
var balanced_expr = a("balanced_expr")

var auto_create_operator = seq(
	"create",
	a("operator"),
	a(opt(id, ".")),
	a(operator),
	"(",
	zero_or_more(not(")"), either(
		seq("leftarg", "=", asIs("left", id)),
		seq("rightarg", "=", asIs("right", id)),
		until(either(",", ")")),
	), opt(",")),
	")",
	until_opt(";"),
).Produce("drop", acc, "(", groupDef("left", "none"), ",", group("right"), ")", ";")

var auto_create = seq("create",
	either(
		// ACCESS METHOD
		// https://www.postgresql.org/docs/18/sql-create-access-method.html
		// https://www.postgresql.org/docs/18/sql-drop-access-method.html
		a("access", "method", id),

		// AGGREGATE, FUNCTION
		// https://www.postgresql.org/docs/18/sql-createfunction.html
		// https://www.postgresql.org/docs/18/sql-dropfunction.html
		// https://www.postgresql.org/docs/18/sql-createaggregate.html
		// https://www.postgresql.org/docs/18/sql-dropaggregate.html
		seq(
			a(either("function", "aggregate"), id, "("),
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

		// OPERATOR CLASS
		// https://www.postgresql.org/docs/18/sql-createopclass.html
		// https://www.postgresql.org/docs/18/sql-dropopclass.html
		seq(
			a("operator", "class", id),
			opt("default"),
			"for", "type", id,
			a("using", id),
		),

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
		a("table", id),

		// ROLE
		// https://www.postgresql.org/docs/18/sql-createrole.html
		// https://www.postgresql.org/docs/18/sql-droprole.html
		a("role", id),

		// VIEW
		// https://www.postgresql.org/docs/18/sql-createview.html
		// https://www.postgresql.org/docs/18/sql-dropview.html
		a(opt("materialized"), "view", id),

		// EXTENSION
		// https://www.postgresql.org/docs/18/sql-createextension.html
		// https://www.postgresql.org/docs/18/sql-dropextension.html
		a("extension", id),

		// DOMAIN
		// https://www.postgresql.org/docs/18/sql-createdomain.html
		// https://www.postgresql.org/docs/18/sql-dropdomain.html
		a("domain", id),

		// CONSTRAINT
		// https://www.postgresql.org/docs/18/sql-createconstraint.html
		// https://www.postgresql.org/docs/18/sql-dropconstraint.html
		seq(opt("constraint"),
			a("trigger", id),
			either("before", "after", seq("instead", "of")),
			either(
				"insert",
				seq("update", opt("of", id, zero_or_more(",", a(id)))),
				"delete",
				"truncate",
			),
			a("on", id),
		),
	),
	// Get everything until a terminating ;
	until_opt(";"),
).Produce("DROP", acc, ";")

var auto_alter_table = seq("alter", "table", id,
	either(
		seq("add", "column", id),
		seq("alter", "column", id, "set", "default"),
		seq("add", "constraint", id),
		seq("rename", "constraint", id, "to", id),
	),
	until_opt(";"),
)

var auto_grant = seq(
	"grant",
	either(
		seq(a(id), "to", asIs("to", id2, zero_or_more(",", id))),
		seq(
			a(until("on")),
			a("on"),
			a(either(
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
			)),
			a(id),
			"to",
			asIs("to", id, zero_or_more(",", id)),
		),
	),
).Produce("revoke", acc, " from", group("to"), ";")

var AutoDowner = either(auto_create_operator, auto_create, auto_alter_table, auto_grant)

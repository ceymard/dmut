package mutations

var id = token("", SqlLexer.Symbols()["Id"])
var id2 = token("", SqlLexer.Symbols()["Id"])
var acc = groupInclude("")
var balanced_expr = a("balanced_expr")

var auto_create = seq("create",
	either(
		a("aggregate", id),
		a("type", id),
		a("role", id),
		a("schema", id),
		a("table", id),
		seq(opt("unique"), a("index"), opt("concurrently"), a(id)),
		a(opt("materialized"), "view", id),
		a("extension", id),
		a("domain", id),
		a("event", "trigger", id),
		a("trigger", id),
		a("foreign", "data", "wrapper", id),
		a("foreign", "table", id),
		a(opt("trusted"), opt("procedural"), "language", id),
		// function
		seq(
			a("function", id, "("),
			zero_or_more(seq(
				either("in", "inout", "out", "variadic"),
				id,
				opt(seq(not("default"), id)),
				opt(either("=", "default")),
				until(either(",", ")")),
				a(opt(",")),
			)),
			a(")"),
		),
	),
	// Get everything until a terminating ;
	until_opt(";"),
).Produce("DROP ", acc, ";")

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
		seq(id, "to", id2), // .Produce(id2, "from", id),
		seq(
			a(until("on")),
			"on",
			a(either(
				"table",
				seq(opt("materialized"), "view"),
				"schema",
				"foreign", "server",
				"tablespace",
				"foreign", "data", "wrapper",
				"database",
				"sequence",
				"function",
			)),
			id,
			a("to"), //.Produce("from"),
			id,
		),
	),
) //.Produce("revoke", acc)

var AutoDowner = either(auto_create, auto_alter_table, auto_grant)

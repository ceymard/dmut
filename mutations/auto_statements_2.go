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
		seq(
			opt("constraint"),
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
		a("foreign", "data", "wrapper", id),
		a("foreign", "table", id),
		a(opt("trusted"), opt("procedural"), "language", id),
		// function
		seq(
			a("function", id, "("),
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
		a("policy", id, "on", id),
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
		seq(a(id), "to", asIs("to", id2, zero_or_more(",", id))), // .Produce(id2, "from", id),
		seq(
			a(until("on")),
			a("on"),
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
			a(id),
			"to",
			asIs("to", id, zero_or_more(",", id)),
		),
	),
).Produce("revoke", acc, " from", groupInclude("to"), ";")

var AutoDowner = either(auto_create, auto_alter_table, auto_grant)

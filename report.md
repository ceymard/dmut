# dmut review — consumer perspective

Scope: does the tool behave consistently with its own documentation, and is it
credible for adoption (dev vs. production). Remaining open items only — see
git history for the resolved items (cycle-detection path, stale FIXME,
`Merge` OR-semantics doc, `ReadAndRunMutations` variadic signature,
`--override` docs, Scope/Namespaces README sections, `RunAllMutations`
transaction leak on error, pgx/v4 removed from the module).

## Open

### 8. Auto-down coverage — constructs worth checking

`mutations/auto_parser.go` covers a documented subset of `CREATE`/`ALTER
TABLE`/`GRANT` forms, confirmed sufficient for ~98% of everyday use by
design (fail-loud on anything outside it, grow coverage as real statements
are hit). Worth checking against current coverage if/when encountered:
partitioned tables, `CREATE INDEX CONCURRENTLY`, `ALTER TYPE ... ADD VALUE`,
identity columns, exclusion constraints.

## Verdict

The core model (hash mutations, diff against recorded state, down/up the
delta and everything downstream, replay every mutation independently to
catch non-invertible changes) is coherent, and the code does what the README
says. Credible for dev and small-to-mid production schemas today. Item #8 is a
coverage watchlist, not a bug — no other open correctness items.

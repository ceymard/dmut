# dmut review — consumer perspective

Scope: does the tool behave consistently with its own documentation, and is it
credible for adoption (dev vs. production). All findings from this review have
been resolved — see git history (cycle-detection path, stale FIXME, `Merge`
OR-semantics doc, `ReadAndRunMutations` variadic signature, `--override`
docs, Scope/Namespaces README sections, `RunAllMutations` transaction leak
on error, pgx/v4 removed from the module, `ATTACH PARTITION` auto-down plus
README notes on `ALTER TYPE ... ADD VALUE` and `CREATE INDEX CONCURRENTLY`).

## Open

None.

## Verdict

The core model (hash mutations, diff against recorded state, down/up the
delta and everything downstream, replay every mutation independently to
catch non-invertible changes) is coherent, and the code does what the README
says. Credible for dev and small-to-mid production schemas today.

# Mutation model rewrite (v3)

Branch `mutation-model`. The model (types, loader, planner, SQL knowledge) is `mutations/model`; the database side (runner, storage, executor) is `mutations`. The root commands do not build until stage 6. The module path becomes v3 in stage 7. `command_legacy.go` reads the v1 format and stays.

- [x] 1. Types and loader: file layout, statements, default-list checks, settings parsing, dependency patterns and cycles, statement fingerprints. No database.
- [x] 2. Planner: pure function from recorded state, new definitions and mode to an ordered list of actions. Triggers, tails, ordering, modes.
- [x] 3. Storage: new `__dmut__` definition (three tables), single read query, tail replace and append.
- [ ] 4. Executor: settings resolution and `set_config`, seeds, checks, `data` rows, `force` retry, logging fields.
- [ ] 5. Test phase: three steps in a rolled-back transaction.
- [ ] 6. Commands and API: `-s`/`-f`/`--set`, `Values`/`Settings` options, `collect`, `explode`, `down`, `overwrite`, revision documents (`mode`, `delete`, `override`).
- [ ] 7. Swap and docs: delete the old package, module path to v3, replace the docs pages (structural mutations, dependencies and reapply triggers, seeds, settings), add a page that migrates v2 source files (`core`/`glue`) to the new format, update examples and README.

## Notes for later stages

- Stage 4: the executor must hold back `begin`... (none). Setting names are checked at load time against what Postgres accepts (tested: a hyphen and a leading digit are refused).
- Stage 2: `override:` entries are recorded, never run; their `deps` patterns still need matched lists at record time.
- Stage 1 follow-ups: `DROP TYPE` cannot be checked for composite-only at load time, so it is accepted in default mutations.
- Stage 1 decisions to confirm: `*` and `**` only match whole name segments (`a*` is a load error); a plain string with several SQL statements keeps the old behaviour (the down is inferred from the first); an empty yaml file is ignored; a file without `namespace` is in the namespace named "".

# CLI reference

```
dmut apply <uri> <paths...>       apply mutations to a database
  -d, --dry                         run everything, then roll back instead of committing
  -o, --override                    record the mutations without applying them (see below)

dmut test <paths...>              apply and test against a throwaway postgres (requires Docker)
  -a, --all                         replay every revision from 1, not just the latest
  --test-image <image>              postgres image to run (default: postgres:14)
  --test-database <name>            (default: test)
  --test-username <name>            (default: test)
  --test-password <pass>            (default: test)

dmut down <uri> <namespace>       down everything dmut applied in a namespace
  -d, --dry

dmut collect <outfile> <paths...> write every mutation back out as a single yaml file
dmut explode --out-dir <dir> <paths...>  split a single-document yaml file into one file per mutation
dmut legacy <uri>                 dump a pre-1.0 dmut schema as yaml
dmut version

global flags (accepted before or after the command; see "Logging" below)
  -j, --json                        log as JSON lines instead of plain text
  -v, --verbose                     log at debug level: every SQL statement as it runs
  --source                          add the source file and line to each log record
  --time                            show timestamps in plain text logs
  --color auto|always|never         colorize plain text logs (default: auto)
```

`<uri>` is a libpq/pgx connection string (`postgres://user:pass@host/db`).

`<paths...>` are files or directories. Directories are walked recursively and every `.yml` /
`.yaml` file found is read.

The default namespace is the empty string, so downing unnamespaced mutations reads:

```sh
dmut down postgres://localhost/mydb ""
```

Short flags are consistent across commands: `-d` is always `--dry`, `-o` is always `--override`. Options that don't share meaning with another command (`test`'s
`--test-*` flags, `explode`'s `--out-dir`) have no short form, so a short flag never means two
different things depending on which subcommand you're running.

## Logging

Everything dmut says about its progress goes to **stderr**, through Go's `log/slog`. Stdout only
carries a command's result (the yaml of `collect -`, the file names `explode` writes, the version),
so `dmut collect - ./mutations > all.yml` is always safe.

By default the log is plain text, with an icon in front of the main milestones (🧪 tests started, 🆗 tests passed, 🎉 no errors, ✅ committed, ⛔ rolled back after a failure):

```
INF applying mutations namespace=api revision=3
INF run namespace=api revision=3 mutation=api.users dir=up kind=core
INF 🧪 performing tests namespace=api revision=3
INF 🆗 tests passed namespace=api revision=3
INF 🎉 no errors
INF ✅ committed
```

With `-j`/`--json` every record is one JSON object per line instead, with the same attributes as
separate fields (and always a `time`). The icons are not part of JSON records, so `msg` stays plain text (`"committed"`):

```
{"time":"…","level":"INFO","msg":"run","namespace":"api","revision":3,"mutation":"api.users","dir":"up","kind":"core"}
```

| Attribute | Meaning |
|---|---|
| `namespace` | namespace of the mutation or revision (`""` is the default namespace) |
| `revision` | revision being applied |
| `mutation` | mutation name |
| `dir` | `up` (green) or `down` (red) |
| `kind` | `core` (the SQL half, magenta) or `glue` (the meta half, blue) |
| `icon` | the milestone icon; shown in front of the message in plain text and removed from JSON by the dmut CLI (a library caller's own handler sees it as an ordinary attribute) |
| `phase` | `test` on records replayed from a failed test phase |
| `sql` | the statement, at debug level (`-v`), or the failing one in an error |
| `uri` | the connection URI, with its password replaced by `xxxxx` |

Plain text logs have no timestamp unless `--time` is given, and are colored only when stderr is a
terminal and `NO_COLOR` is unset (`--color=always` / `--color=never` override this). `--source`
adds the Go source file and line to every record; it is only useful for debugging dmut itself.

The test phase is silent when it succeeds. When it fails, everything it did is printed after the
fact, with `phase=test`, so that the failing statement can be seen in context.

A rollback that is just the end of a dry run or of `dmut test` is logged as `rolling back, nothing to commit`, without the ⛔ icon.

When a command fails, the error is logged last at error level, without a stack trace. For a
Postgres error the message, `detail`, `hint` and `sqlstate` come first, followed by the failing
statement indented on its own lines (in color, the part from the error position onwards is red).
In JSON mode that is a single record with `error`, `detail`, `hint`, `sqlstate`, `position` and the
raw `sql` as fields.

### Using dmut as a library

`mutations.NewPgRunner(uri, logger)` and `MutationRunnerOptions.Logger` take a `*slog.Logger`; when
it is nil, `slog.Default()` is used. `mutations.ErrorAttrs(err)` flattens an error and its context
into slog arguments.

## `--override`

`--override` records the mutations as applied *without running them*. It exists for a database
dmut is **already** managing, where the recorded definitions turn out to be wrong — usually
because a mutation's `up`/`down` pair didn't actually round-trip cleanly in some situation the test
phase didn't catch, and re-applying or downing it the normal way would now fail or do the wrong
thing. `--override` lets you hand dmut the corrected definition and have it accept that as the new
source of truth for that mutation, without running the old or new SQL against the objects
themselves — you already know the objects are in the state the corrected definition describes.

`--override` is **never** appropriate on a new or empty database. It is not a seeding mechanism; it
corrects the record for objects that already exist and are already tracked. On a fresh database,
use a plain `apply` — that runs the mutations for real.

The test phase still runs after an override, deliberately: the dmut contract is that anything dmut
knows about must be downable, and an overriding definition is no exception, so it gets the same
down-and-reup-behind-savepoints scrutiny as a normal apply before you can trust it. If the
override's corrected `down` still doesn't round-trip, the test phase is what catches that.

`--override` also cannot be the *first* thing dmut does on a database: it skips creating its own
bookkeeping schema along with everything else, since that step assumes there's already a recorded
state to correct. Run a plain apply once before adopting objects with `--override`.

## Bookkeeping

Dmut keeps its own bookkeeping in the `__dmut__` schema (table `__dmut__.mutations`), which it
creates on first run as a namespace of its own.

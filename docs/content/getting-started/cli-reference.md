# CLI reference

```
dmut apply <uri> <paths...>       apply mutations to a database
  -d, --dry                         run everything, then roll back instead of committing
  -o, --override                    record the mutations without applying them (see below)
  -s, --safe                        refuse any change that is not an append
  -f, --force                       if a change fails, reapply what it blocks and try again
  -a, --all                         on an empty database, replay every revision from the first
  --set N=value                     value of the setting dmut.values.N (repeatable)

dmut test <paths...>              apply and test against a throwaway postgres (requires Docker)
  -a, --all                         replay every revision from 1, not just the latest
  --test-image <image>              postgres image to run (default: postgres:14)
  --test-database <name>            (default: test)
  --test-username <name>            (default: test)
  --test-password <pass>            (default: test)

dmut down <uri> <namespace>       down everything dmut applied in a namespace
  -d, --dry

dmut collect <outfile> <paths...> write every mutation back out as a single yaml file
  -r, --revision <n>                write a revision shell at this number instead (see below)
dmut explode --out-dir <dir> <paths...>  write one file per mutation
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

`-s` and `-f` exclude each other. See [Run modes](how-a-run-works.md#run-modes).

`--set` gives a value to a [setting](../mutation-format/settings.md) named `dmut.values.N`. It can be
repeated, and a value may contain commas: `--set greeting="hello, world"`. Settings named
`dmut.env.X` read the environment variable `X`.

Short flags are consistent across commands: `-d` is always `--dry`, `-o` is always `--override`,
`-a` is always `--all`. Options that don't share meaning with another command (`test`'s `--test-*`
flags, `explode`'s `--out-dir`) have no short form, so a short flag never means two different
things depending on which subcommand you're running.

`dmut collect -r <n>` writes, instead of a flat collect, a *revision shell* at revision `n`: the
current definition of each namespace with room to write `override:` and `delete:`, ready to be
filled in. Given a single `postgres://` URI instead of paths, it sources the shell from what the
database has recorded; data of seeds and settings that are neither tracked nor saved are not stored,
so they are left out with a warning. See [Revisions](../revisions/index.md).

## Logging

Everything dmut says about its progress goes to **stderr**, through Go's `log/slog`. Stdout only
carries a command's result (the yaml of `collect -`, the file names `explode` writes, the version),
so `dmut collect - ./mutations > all.yml` is always safe.

By default the log is plain text, with an icon in front of the main milestones (🧪 tests started, 🆗 tests passed, 🎉 no errors, ✅ committed, ⛔ rolled back after a failure). Every `run` record also starts with 🟢 when it goes up or 🔴 when it goes down:

```
INF applying mutations namespace=api revision=3
INF 🟢 run namespace=api revision=3 mutation=api.users dir=up
INF 🧪 performing tests namespace=api revision=3
INF 🆗 tests passed namespace=api revision=3
INF 🎉 no errors
INF ✅ committed
```

With `-j`/`--json` every record is one JSON object per line instead, with the same attributes as
separate fields (and always a `time`). The icons are not part of JSON records, so `msg` stays plain text (`"committed"`):

```
{"time":"…","level":"INFO","msg":"run","namespace":"api","revision":3,"mutation":"api.users","dir":"up"}
```

| Attribute | Meaning |
|---|---|
| `namespace` | namespace of the mutation or revision (`""` is the default namespace) |
| `revision` | revision being applied |
| `mutation` | mutation name |
| `dir` | `up` (green) or `down` (red) |
| `reason` | on a `run` record caused by something other than an edit: the trigger (`dep_reapplied`, `dep_appended`, `deps_grow`, `deps_shrink`) or `setting` |
| `cause` | the dependency or the setting behind `reason` |
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

`mutations.Apply(ctx, uri, paths, mutations.Options{...})` loads the files and applies them in one
call. `mutations.NewRunner(ctx, uri, logger)` gives a connection to drive by hand, with
`LoadProject`, `ApplyProject`, `DownNamespace` and the `Collect` functions. `Options.Logger` takes a
`*slog.Logger`; when it is nil, `slog.Default()` is used. `mutations.ErrorAttrs(err)` flattens an
error and its context into slog arguments.

`Options` mirrors the flags: `Mode` (`model.ModeSafe`, `ModeNormal`, `ModeForce`), `Commit`,
`Override`, `All`, and for settings `Values` (the `dmut.values.N` map) and `Settings`, a
`func(path []string) *string` called for any setting that is neither `dmut.env.X` nor
`dmut.values.N`, with the name split on dots; return nil for unset.

Every function that talks to the database takes a `context.Context` as its first argument.
Cancelling it, or letting its deadline expire, aborts the statement in flight and makes the call
return an error that wraps `ctx.Err()` (`errors.Is(err, context.Canceled)` works). The run is then
rolled back, so nothing is applied: the rollback and `Close` end on a fresh context that ignores the
cancellation, each waiting at most `mutations.CleanupTimeout` (5 seconds). If the database does not
answer in time, the connection is dropped and Postgres rolls the transaction back by itself.
`NewRunner` uses its `ctx` only to connect.

The `dmut` command cancels this context on Ctrl-C (SIGINT) and SIGTERM.

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

`--override` still creates dmut's own bookkeeping schema when it is missing, since that is what it
records into.

## Bookkeeping

Dmut keeps its own bookkeeping in the `__dmut__` schema, which it creates on first run as a namespace
of its own, from three tables:

- `__dmut__.mutations`: one row per mutation, with its file, revision, `structural` flag and
  `reapply_on`.
- `__dmut__.statements`: one row per statement, with its kind, its `up` and `down`, a digest of its
  data and tracked settings, and the values of its saved settings.
- `__dmut__.deps`: one row per entry of `deps`, with the names it matched at the last run.

A database written by an earlier version of dmut cannot be read: dmut refuses it and tells you so.
See [Migrating from dmut 2](../migrating.md).

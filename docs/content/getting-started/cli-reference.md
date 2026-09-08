# CLI reference

```
dmut apply <uri> <paths...>       apply mutations to a database
  -v, --verbose                     echo every statement as it runs
  -d, --dry                         run everything, then roll back instead of committing
  -o, --override                    record the mutations without applying them (see below)

dmut test <paths...>              apply and test against a throwaway postgres (requires Docker)
  -v, --verbose
  -a, --all                         replay every revision from 1, not just the latest
  --test-image <image>              postgres image to run (default: postgres:14)
  --test-database <name>            (default: test)
  --test-username <name>            (default: test)
  --test-password <pass>            (default: test)

dmut down <uri> <namespace>       down everything dmut applied in a namespace
  -v, --verbose
  -d, --dry

dmut collect <outfile> <paths...> write every mutation back out as a single yaml file
dmut explode --out-dir <dir> <paths...>  split a single-document yaml file into one file per mutation
dmut legacy <uri>                 dump a pre-1.0 dmut schema as yaml
dmut version
```

`<uri>` is a libpq/pgx connection string (`postgres://user:pass@host/db`).

`<paths...>` are files or directories. Directories are walked recursively and every `.yml` /
`.yaml` file found is read.

The default namespace is the empty string, so downing unnamespaced mutations reads:

```sh
dmut down postgres://localhost/mydb ""
```

Short flags are consistent across commands: `-v` is always `--verbose`, `-d` is always `--dry`,
`-o` is always `--override`. Options that don't share meaning with another command (`test`'s
`--test-*` flags, `explode`'s `--out-dir`) have no short form, so a short flag never means two
different things depending on which subcommand you're running.

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

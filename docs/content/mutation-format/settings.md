# Settings

A setting is a named value that dmut gives to a statement when it runs: an environment variable, a
value from the command line, or anything your program provides. The statement reads it with
`current_setting('name')`.

```yaml
structural: true
sql:
  - create table events (kind text)
  - seed: insert into events values (current_setting('dmut.values.environment'))
    settings: [dmut.values.environment]
```

```sh
dmut apply postgres://localhost/mydb ./mutations --set environment=staging
```

## Naming and sources

A statement lists its settings with `settings`. An entry is a name, or a mapping
`{name: <name>, tracked: <bool>, saved: <bool>}`. A name starts with `dmut.` and has at least one
more dot. Each part starts with a letter or `_` and holds letters, digits, `_` and `$`, because
Postgres refuses other names.

- `dmut.env.X` is the environment variable `X`.
- `dmut.values.N` is the value of `--set N=<value>` (the `Values` option for a library caller).
- Any other name is asked of the callback of the program that embeds dmut, with the name split on
  dots (`["dmut", "app", "color"]`). Without a callback the setting is unset. The command line has no
  callback.

Dmut resolves every name once per run, before the transaction starts. A name can be *unset*, which
is not the same as empty for dmut, but reads the same in SQL.

## What a statement sees

- Before a statement runs, dmut sets each name it lists. After the statement, those names are empty
  again, so a setting never leaks to the next statement.
- A name that a statement does not list reads as the empty string, provided another statement lists
  it. A name that no statement lists is unknown to Postgres and `current_setting` raises an error:
  that is the safety net against a typo.
- An unset name reads as the empty string. If you want an error instead, write
  `nullif(current_setting('dmut.env.X'), '')` and `raise exception` in the statement.

## Tracked settings

A change of the value of a **tracked** setting (the default) is a change of the statement: the tail
from that statement goes down and up again, with `reason=setting` and the name in `cause`. Dmut
stores, with the statement, the name of each tracked setting and a hash of its value (with the
namespace, the mutation and the position mixed in), never the value. Adding, removing or reordering
an entry counts as a change. A value that becomes unset changes the digest, even compared to an
empty value: a deploy job that lacks a variable redoes the tail with an empty value, which is
intended.

`tracked: false` stores nothing and never causes a redo.

A hash is not a protection for a weak secret: someone who can read the database can test guesses
against it. Use `tracked: false` for such a value.

## Settings in downs

A down does not see the current value of a setting. It sees empty values, except for the settings
that were **saved**:

```yaml
structural: true
sql:
  - up: do $$ begin execute format('create table %I (id int)', current_setting('dmut.values.prefix') || '_events'); end $$
    down: do $$ begin execute format('drop table %I', current_setting('dmut.values.prefix') || '_events'); end $$
    settings: [{name: dmut.values.prefix, saved: true}]
```

`saved: true` stores the value the up used, in clear, with the statement, and hands it to the down.
So when the value changes, the tail redo drops the table of the *old* value, and the up creates the
table of the new one. A down that needs a value and does not save it fails in the test phase, at the
first apply, which is where you want to find out.

Never save a secret: the value sits in the database. Dmut warns, when the files are loaded, about a
saved setting whose name has a part that contains `secret`, `password`, `passwd`, `pwd`, `token`,
`credential`, `private`, `auth`, `apikey` or `key`.

`tracked` and `saved` are independent. A setting that is saved but not tracked is stored for its
down, and a change of its value redoes nothing.

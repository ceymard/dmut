# Dependencies and reapply triggers

## `deps`

`deps` lists what a mutation depends on. An entry is an exact mutation name, a pattern, or a mapping
`{name: <name or pattern>, reapply_on: [...]}`.

```yaml
deps:
  - api.users                # exact name
  - 'types.*'                # every mutation one level under types
  - {name: 'views.**', reapply_on: [deps_grow]}
```

- An exact name that does not exist in the namespace and revision is an error.
- In a pattern, `*` and `**` are whole segments of the dotted name. `*` matches one segment,
  `**` any number of them (including none). They are the only pattern characters.
- A pattern that matches nothing is allowed. A mutation never matches its own pattern.
- A dependency cycle is an error, and the error names the mutations in the cycle.
- A mutation also depends on each existing ancestor in its dotted name (`a.b.c` on `a.b` and `a`).
- A dependency cannot cross a namespace or a revision.

Mutations go up in dependency order and come down in the reverse order. Dmut orders the downs with
the new definitions; a mutation that no longer exists comes down in full, ordered with the
dependencies recorded for it.

Naming a mutation in `deps` does not pull in the mutations dotted underneath it: depend on what you
use.

A changed `deps` list is adopted at once and takes no action by itself. Only a [trigger](
#reapply-triggers) can make something run because of it.

## Reapply triggers

To *reapply* a mutation is to take it down in full and bring it up in full. Four events can make a
mutation reapply:

| Trigger | Fires when |
|---|---|
| `dep_reapplied` | A dependency of the entry is reapplied (down in full and back up), or comes down because it was removed. |
| `dep_appended` | Statements are appended to a dependency of the entry, and nothing before them changed. |
| `deps_grow` | The entry now matches a mutation it did not match at the last run. |
| `deps_shrink` | The entry no longer matches a mutation it matched at the last run. |

A mutation has a set of triggers for each dependency, taken in this order: the `reapply_on` of the
entry, else the `reapply_on` of the mutation, else the default: `[dep_reapplied]` for a default
mutation, nothing for a [structural](structural.md) one. When two entries match the same dependency,
the set that applies is the union. The dotted-name parent counts as a dependency with the
mutation's own set.

```yaml
# a view that must follow the columns of its table
deps: [accounts]
reapply_on: [dep_reapplied, dep_appended]
sql:
  - create view account_names as select * from accounts
```

- A mutation reapplies when any of its triggers fires in the same run. When several fire, the
  `reason` logged is the first of `dep_reapplied`, `dep_appended`, `deps_grow`, `deps_shrink`.
- When a mutation reapplies, the mutations that depend on it are checked for `dep_reapplied`, in
  dependency order, so a chain follows.
- A mutation is reapplied once, whatever the number of triggers. If its own statements also changed,
  the reapply covers them.
- A mutation that is new in the run has no triggers fired for it.
- A tail edit of a mutation is not a reapply: the dependents are not touched. If something blocks
  the tail from coming down, the run fails in the mode `normal`; declare the dependency with a
  trigger, or use `-f`.
- The names an entry matched are stored with the mutation, to detect `deps_grow` and `deps_shrink`.
  An entry that is new has an empty stored list, and an entry that is gone is compared with an
  empty one, with the mutation's own triggers.

A reapply of a structural mutation drops what it created, with the data: `dmut apply -s` refuses it,
and `reapply_on` on a structural mutation should be used with that in mind.

## Run modes and reapplies

In the mode `force`, when the plan fails, dmut rolls back to a savepoint and reapplies the mutations
that had to go down together with *everything that depends on them*, whatever their `reapply_on`
says, then tries once more. See [Run modes](../getting-started/how-a-run-works.md#run-modes).

# How a run works

Everything happens inside a single transaction, per namespace, in ascending revision order. A
failure at any step stops the process and nothing is applied.

1. **Read** the mutations already applied for that namespace from the `__dmut__` tables, in one
   query.
2. **Plan** what has to change, without touching the database. Each mutation is compared with its
   recorded version, statement by statement and in order:
    - nothing differs: nothing happens;
    - statements were added at the end (an *append*): only the new statements go up;
    - a statement differs from the recorded one: the *tail* — that statement and every one after it
      — comes down, and the new tail goes up. Statements before it are left alone, seeds included;
    - the mutation is new: all of it goes up;
    - the mutation no longer exists: all of it goes down.

   Then the plan adds the *reapplies*: a mutation whose dependencies changed may have to come all the
   way down and back up, depending on the [triggers](../mutation-format/dependencies.md#reapply-triggers)
   it asked for. A reapply of one mutation can in turn trigger those that depend on it.
3. **Check the mode.** With `-s` (`safe`), any change that is not an append is refused here, before
   anything runs. See [Run modes](#run-modes).
4. **Run** the plan. All downs first, dependents before their dependencies, then all ups,
   dependencies before their dependents. Each statement runs with the [settings](
   ../mutation-format/settings.md) it lists. Seeds run every time their part of the mutation goes
   up, and checks must return `true`.
5. **Record** the new state in the `__dmut__` tables: the mutations, their statements, their
   dependencies and what each dependency matched.
6. **Test**, when something ran. With everything up, each mutation is taken down in full together
   with all its dependents and brought back up, and the changes are rolled back. Then every
   mutation goes down and up again in one go, running each seed twice in a row. This runs behind
   savepoints and is rolled back, so no data is lost, but every mutation must be cleanly downable
   or the whole apply aborts, naming the mutation and the statement. Typical causes are a missing
   dependency, a wrong `down`, or a seed that is not safe to run twice.
7. **Commit**, or roll back if `-d` was given.

A revision whose `override:` records something other than what ran is tested a second time in its
recorded form, so that what the *next* revision inherits is known to work. See
[Revisions](../revisions/index.md).

`dmut test` runs the same sequence against a throwaway Postgres container and never commits.

## Run modes

A run has one mode. The command line sets it, and a [revision document](../revisions/index.md) can
set its own with `mode:`, which wins.

| Mode | Flag | Behaviour |
|---|---|---|
| `safe` | `-s` | Refuses, before running anything, every change that is not an append: a changed statement, a removed mutation, a reapply of a structural mutation, a change of the `structural` flag. This is the mode for an automated pipeline. |
| `normal` | none | Takes the tail down and brings the new tail up. A Postgres error aborts the run. |
| `force` | `-f` | Tries the plan behind a savepoint. If it fails, rolls back to the savepoint, reapplies in full every mutation that had to go down and everything that depends on it, whatever their triggers say, and tries once more. |

In `normal` mode, a change that cannot go down because something else uses what it created (a
function used by a view, a table referenced by another one) fails with the Postgres error. Either
declare the dependency so that the dependent is reapplied, or run with `-f` for this once.

A reapply of a structural mutation drops the objects it created, with their data.

## Watching it happen

Run any of the examples in this documentation with `-v` against a real database (or `dmut test -v`
against a throwaway one) to see this sequence directly: every statement dmut runs, in order, gets
echoed. Each mutation that goes down or up is also one `run` record, with the trigger that caused a
reapply in `reason` and the dependency or setting behind it in `cause`. See the [CLI
reference](cli-reference.md#logging).

# dmut

A Postgres migration tool. It keeps a database in line with a set of mutations written in yaml, working out what has to come down and go back up when the set changes.

## Language

**Mutation**:
A named unit of SQL statements with the dependencies it needs, applied to and removed from a database as a whole.
_Avoid_: migration, change, step

**Namespace**:
An isolated set of mutations on a database. Mutations never depend on, or touch, another namespace's.
_Avoid_: schema, project

**Revision**:
A numbered version of a namespace's set of mutations. A database is carried across revisions in ascending order. Mutations that belong to no numbered revision are at the unnamed revision, the one after the highest number.
_Avoid_: version, release

**Structural mutation**:
A mutation that creates or alters things whose removal loses data or is slow to redo (tables, columns, indexes, types, schemas, extensions). It is declared as such, and it is never reapplied automatically unless it asks.
_Avoid_: core, schema mutation, heavy mutation

**Dependency**:
A mutation (or a group of mutations picked by a name pattern) that another mutation lists in its `deps`. It decides the order mutations go up in, and what a mutation may reapply on.
_Avoid_: needs, parent, after, glue_needs

**Tail**:
The first statement of a mutation that differs from the recorded version, and every statement after it. When a mutation changes, its recorded tail comes down and its new tail goes up; the statements before the tail are left alone.
_Avoid_: delta, suffix, changed part

**Append**:
A change that only adds statements at the end of a mutation. The recorded tail is empty, so nothing comes down.
_Avoid_: pure append, extension

**Reapply**:
To take a mutation down and bring it back up in full, not only its changed tail.
_Avoid_: rebuild, redo, rerun, refresh, replay

**Reapply trigger**:
A named event, listed in `reapply_on`, that makes a mutation reapply: a dependency was reapplied (`dep_reapplied`), a dependency had statements appended (`dep_appended`), or its set of dependencies grew (`deps_grow`) or shrank (`deps_shrink`).
_Avoid_: rebuild condition, cascade rule

**Setting**:
A named value handed to a statement when it goes up, by the environment or by the program using dmut. A change of its value makes the tail that starts at that statement go down and up again, unless the setting is untracked. A saved setting is also stored in clear and handed to the down of the statement; any other setting reads as empty in a down.
_Avoid_: variable, parameter, secret, env

**Seed statement**:
A statement with no way back: it is skipped when its mutation goes down, run again whenever the mutation goes up, and must be safe to run more than once. The author promises that; the test phase runs it twice.
_Avoid_: idempotent statement, one-way statement, forward-only

**Check statement**:
A statement that asserts a condition: it runs a query when its mutation goes up, and the run fails if the query does not return true. It changes nothing and has no way back.
_Avoid_: ensure statement, assertion, test

**Run mode**:
How a run reacts to a change that is not an append: `safe` refuses it, `normal` runs it and stops on a Postgres error, `force` falls back to reapplying the mutation and its dependents.
_Avoid_: default mode, simple mode, full mode

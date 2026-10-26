# Mutation model

This document defines a single kind of mutation that replaces the `core` and `glue` blocks. Terms are defined in `CONTEXT.md`. Words in the "Avoid" lists of `CONTEXT.md` must not appear in new code, new documentation, log messages or error messages.

> 📜 **ADR**: The `core`/`glue` split ordered mutations globally (all `core`, then all `glue`) and rebuilt all `glue` after any `core` change. Named dependencies with per-dependency triggers replace both rules. Dotted names already give the parent dependency, and appending statements to a parent does not take anything down, so the main cost of separate mutations (a new mutation for each added column) is gone.

This document defines no migration from `core` and `glue`. A database written by an earlier version must be reset.

## 1. Mutation file format

A mutation file has these document-level keys: `namespace`, `revision`, and the revision keys `mode`, `delete`, `override` and `mutations`. The earlier keys `__namespace` and `__revision` lose their prefix. The layout of a file is in "File layout" below.

A mutation has these keys. Any other key is a load error.

| Key | Meaning |
|---|---|
| `sql` | Ordered list of statements (section 2). |
| `structural` | Boolean, default `false` (section 3). |
| `deps` | List of dependency entries (section 4). |
| `reapply_on` | List of reapply triggers, a default for every entry of `deps` (section 5). |

> 📜 **ADR**: The key `children` is removed. An append to `sql` adds a column without a new mutation, and a dotted file name (`api.project.table.yml`) gives the same name and the same parent dependency. `children` was a second way to define a name.

Any key that is not in this table is a load error. dmut stores the `deps` of the definition that ran.

An `override:` entry replaces `sql`, `structural`, `deps` and `reapply_on` as a whole. A key left out of an `override:` entry is recorded as empty or `false`.

### File layout

1. If the first key of a document is `revision`, the document is a revision document (rule 2). Any other document is a mutation file (rule 3). A document is one YAML document, so a file may hold several of them.
2. A revision document has these keys only, and `revision` comes first: `revision`, `namespace`, `mode`, `delete`, `override`, `mutations`. The mutations are the entries of `mutations:` (a map from mutation name to mutation) and of `override:`. The name of the file has no meaning. `collect` writes this form. No mutation name is reserved, because a name appears only under `mutations:` and `override:`.
3. A mutation file holds one mutation. The name of the mutation is the name of the file without `.yml` or `.yaml` (`api.project.table.yml` holds `api.project.table`). The keys of the file are the keys of the table above and `namespace`. A key not in that list is an error, including `revision` and `mutations`. The directory of the file has no meaning. A file named `revision.yml` is a mutation file that defines the mutation `revision`.
4. A mutation file is at the unnamed revision, which is the revision after the highest numbered revision of its namespace (or `1` when there is none), as today.
5. Two definitions of one name in one namespace and revision are a load error. The error names both files.
6. The name of a file may contain letters, digits, `_`, `-` and dots only. Another name is a load error.
7. A file with no YAML document, or a document that is empty, is ignored with a warning. A file without `namespace` is in the namespace named by the empty string.

`collect` writes the form of rule 2, with every mutation nested under `mutations:`. `explode` writes one file for each mutation, named `<mutation>.yml` in the output directory, with `namespace` and without `revision`.

## 2. Statements

A statement is a plain string, or an object with exactly one of the keys of the table below, and the optional key `settings` (section 7). The key `data` is allowed with `seed` only.

| Form | Meaning |
|---|---|
| plain string | A reversible statement. dmut infers the down. It must be a single SQL statement, and dmut must be able to infer its down. |
| `auto: <sql>` | Same as a plain string. It exists so that the statement can carry `settings`. |
| `up: <sql>` with `down: <sql>` | A reversible statement with an explicit down. The `down` must not be empty. |
| `seed: <sql>` | A seed. It has no down. It may carry `data` (see below). |
| `ensure: <sql>` | A check statement. It has no down. |

Seed rules:

1. A seed runs each time its mutation goes up, and each time the tail that contains it goes up.
2. A seed is skipped when its mutation goes down, and when the tail that contains it comes down.
3. A seed before the first changed statement does not run again when the tail changes.
4. The author must make a seed safe to run more than once.
5. The test phase runs each seed twice in a row (section 9).
6. A seed may appear in any mutation, structural or not.

Seed data:

1. `data` is a list of rows. A row is a list of values. The statement uses the parameters `$1` to `$n`, and `n` must equal the length of every row. A different length is a load error.
2. dmut runs the statement once for each row, in list order, in one batch in the same transaction. With no row, the statement does not run.
3. A statement with `data` must be a single SQL statement.
4. dmut sends each value as text, as it is written in the file, and Postgres reads it for the type of the column, as `COPY` does. A YAML null is `NULL`. A value that is a map or a list is sent as JSON text.
5. The identity of the statement includes a hash of `data`. dmut stores the hash and not the data. A change of `data` is a change of the statement (section 5, rule 4).
6. YAML anchors and aliases work in `data`, because the parser resolves them before dmut reads the data. They work inside one document only. `collect` and `explode` write the data with aliases expanded.

```yaml
sql:
  - seed: insert into roles (id, name, tags) values ($1, $2, $3) on conflict (id) do nothing
    data:
      - [1, admin, '["a", "b"]']
      - [2, guest, null]
```

> 📜 **ADR**: A Postgres parameter holds one value, so `values $1` cannot take a list of rows. Running the statement once for each row keeps the statement plain SQL, and the values never go through string quoting. A list of maps is not used, because a map needs a rule to map keys to parameters. An author who wants a JSON document in one parameter writes a row with one map: `data: [[{a: 1}]]` and `$1::jsonb`.

dmut classes these statements as seeds without the `seed:` prefix: `COMMENT ON ...`, and, in a structural mutation, `ALTER TYPE ... ADD VALUE IF NOT EXISTS ...`. `ALTER TYPE ... ADD VALUE` without `IF NOT EXISTS` is a load error unless the author writes an explicit `down` or the `seed:` prefix.

> 📜 **ADR**: The empty `down` had two meanings: "forgotten" and "intentionally nothing". A separate statement kind removes the second meaning and lets the test phase check the promise of safe repetition.

A check statement is written `ensure: <sql>`. It asserts a condition.

1. The `<sql>` must start with `SELECT` or `WITH`, and must return exactly one row with exactly one column of type boolean. `true` passes. `false` fails the run. Any other result is a run error that names the shape.
2. A check statement has no down. It is skipped when its mutation goes down, as a seed is.
3. A check statement runs each time the tail that contains it goes up, as a seed does. It runs once, also in the test phase.
4. A check statement may carry `settings`. It may appear in any mutation, structural or not.
5. The error of a failed check names the namespace, the mutation, the position and the first words of the statement.

## 3. Structural and default mutations

A default mutation accepts only the statements of the list below. A structural mutation accepts every statement, with one exception (rule 1). dmut checks both at load time, before it connects to a database. The error names the file, the mutation, the position and the first words of the statement. For a default mutation, the error says to set `structural: true`.

**List of a default mutation**

- `CREATE`, `DROP` and `ALTER` (including `OWNER TO` and renames) of functions, procedures, aggregates, views, triggers (including constraint triggers), policies and rules.
- `CREATE TYPE name AS (...)` and `DROP TYPE` of composite types only.
- `ALTER TABLE` with these actions only: `ENABLE`, `DISABLE`, `FORCE` and `NO FORCE ROW LEVEL SECURITY`; `ENABLE` and `DISABLE TRIGGER` (with `REPLICA` or `ALWAYS`); `ALTER COLUMN ... SET DEFAULT` and `ALTER COLUMN ... DROP DEFAULT`.
- `GRANT`, `REVOKE` and `ALTER DEFAULT PRIVILEGES`.
- `COMMENT ON`.
- `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE`, `COPY`, `SELECT`, `WITH`, `DO` and `CALL`.

The body of a `CREATE FUNCTION` and of a `DO` block is not inspected.

Rules:

1. A statement that starts, ends or nests the apply transaction is a load error in every mutation: `BEGIN`, `START TRANSACTION`, `COMMIT`, `END`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`, `PREPARE TRANSACTION`.
2. The default `reapply_on` of a structural mutation is `[]`. The default `reapply_on` of a default mutation is `[dep_reapplied]`.
3. A change of the `structural` flag of a recorded mutation changes no statement and runs nothing. dmut stores the new flag. The new statements must pass the load check of the new kind. The flag changes the default `reapply_on` (rule 2) from the next run on. The change is allowed in the modes `normal` and `force`. In the mode `safe` it is refused.

> 📜 **ADR**: The list exists so that a default mutation, which dmut can reapply on its own, does not receive statements that alter structure, or only a few of them. A structural mutation is the place for everything else, so it has no list.

## 4. Dependencies

An entry of `deps` is one of: an exact mutation name, a name pattern, or an object `{name: <name or pattern>, reapply_on: [...]}`.

1. An exact name that does not exist in the namespace is a load error.
2. A pattern is matched against mutation names in the same namespace. `*` and `**` are whole segments of the pattern (the text between two dots), so `a*` is a load error. `*` matches one name segment. `**` matches any number of segments. They are the only pattern characters.
3. A pattern that matches no mutation is allowed.
4. A mutation never matches its own pattern.
5. A dependency cycle is a load error. The error names the mutations in the cycle.
6. A mutation depends on each ancestor in its dotted name that exists, as today.
7. A dependency cannot cross a namespace.
8. Mutations go up in dependency order and come down in the reverse order. dmut uses the dependency graph of the new definitions to order the ups, to order the downs of every mutation that still exists (including a mutation that comes down to reapply), and to decide which mutations reapply.
9. A mutation that no longer exists in the files comes down in full. dmut orders its down with the dependencies recorded for it, and no others. A mutation that lists a removed mutation fails to load (rule 1).
10. dmut stores, for each entry of each mutation, the list of mutation names the entry matched at the last run.

> 📜 **ADR**: dmut adopts a changed `deps` list at once, before it runs anything, and takes no action on the change unless a trigger of section 5 asks for one. A wrong `deps` list that was applied can be corrected in the next run. The test phase and Postgres detect most wrong lists. A reference that Postgres does not track (for example, a table named in a PL/pgSQL body) is not detected.

> 📜 **ADR**: dmut does not read the old graph to order downs of live mutations. Using both graphs can create a cycle when a dependency is reversed. A removed mutation has no new definition, so it is the only case that uses recorded edges.

## 5. Reapply

To reapply a mutation is to take it down in full and bring it up in full.

A mutation has a set of reapply triggers for each entry of its `deps`. The set for an entry is, in this order: the `reapply_on` of the entry, else the `reapply_on` of the mutation, else the default of section 3. When two entries match the same dependency, the set that applies is the union of the two sets.

| Trigger | Fires when |
|---|---|
| `dep_reapplied` | A dependency of the entry comes down in full and goes back up, or comes down because it was removed. |
| `dep_appended` | Statements are appended to a dependency of the entry, and nothing before them changed. |
| `deps_grow` | The list of mutation names matched by the entry has a name that was not in the stored list. |
| `deps_shrink` | The stored list has a name that the entry no longer matches. |

Rules:

1. A mutation reapplies when any of its triggers fires in the same run. When several fire, the reason is the first in this order: `dep_reapplied`, `dep_appended`, `deps_grow`, `deps_shrink`.
2. When a mutation reapplies, the mutations that depend on it are checked for `dep_reapplied`, in dependency order, with the same rules. A mutation downed because of a trigger is brought back up in the same run.
3. A mutation that reapplies is not also reapplied for a second trigger in the same run.
4. When the statements of a mutation change, dmut takes down the tail and brings up the new tail. When the statements only grow at the end (an append), dmut runs the new statements only. A change of the settings of a statement (section 7) is a change of that statement.
5. dmut detects `deps_grow` and `deps_shrink` by comparing, for each entry, the stored list of the entry with the same pattern with the new list, before it stores the new lists. An entry with a new pattern has an empty stored list. An entry that no longer exists is compared with an empty new list, and the triggers that apply are those of the mutation, or the default.
6. A mutation that is new in this run has no stored lists, and no trigger fires for it.
7. In the mode `force`, a mutation that blocks a dependent tail is handled as today: dmut reapplies the dependents in full, whatever their `reapply_on` says. In the mode `safe`, a reapply of a structural mutation is refused as a non-append change.
8. A reapply of a structural mutation in the mode `normal` drops the objects it created, with their data.
9. A change of the order of the statements of a mutation is a change of the statements (rule 4). The tail starts at the first statement whose position holds a different statement.
10. The triggers of a removed dependency are those of the entries of the mutation that matched it, or the triggers of the mutation when no such entry is left.

> 📜 **ADR**: dmut does not accept a reorder of statements as "no change". Two orders that both run without error can build different databases. Example: `alter table t add column b; alter table t add column c;` swapped gives the column order `(a, c, b)` on a new database and `(a, b, c)` on a database built before. The same happens with `grant` followed by `revoke`, two `create or replace function` of one name, and seeds that write the same rows. The test phase does not compare schemas, so it cannot detect this, and databases built in the old order and in the new order would differ without notice. An author who needs to change the recorded order without running anything uses `override:` in a revision (see the revisions documentation). That makes the divergence an explicit and reviewed decision.

### Run modes

A run has one mode. The command line sets it, and the key `mode` of a revision document overrides it.

| Mode | Flag | Behaviour |
|---|---|---|
| `safe` | `-s` | dmut refuses, before it runs anything, every change that is not an append. |
| `normal` | none | dmut takes the tail down and brings the new tail up. A Postgres error aborts the run. |
| `force` | `-f` | dmut tries the tail behind a savepoint. If it fails, dmut rolls back to the savepoint, reapplies the mutation and every mutation that depends on it, and tries once more. |

The earlier values `default`, `simple` and `full` of `mode` are not accepted.

## 6. Naming

New code, documentation and messages use "reapply" where the earlier code said "rebuild". Existing text that describes `core` and `glue` is removed together with them.

## 7. Settings

A setting is a named value that dmut gives to a statement when it runs.

A statement lists its settings with the key `settings`. An entry is a setting name, or an object `{name: <name>, tracked: <boolean>, saved: <boolean>}`. `tracked` defaults to `true`, and `saved` to `false`. A tracked entry makes a change of its value a change of the statement (rule 8). A saved entry stores its value in clear, and gives it to the down of the statement (rule 6).

1. A setting name starts with `dmut.`, has at least one more dot, and is not case-sensitive in SQL.
2. dmut resolves each name used by a statement in the set of mutations to apply once for each run, before the transaction starts. A name resolves to a string, or to nothing for unset.
3. dmut resolves two kinds of name itself. A name `dmut.env.X` reads the environment variable `X`, spelled as written. A name `dmut.values.N` reads the value of the option `--set N=<value>`, which may repeat. A library caller gives these values with the apply option `Values`, a map from `N` to the value. For any other name, dmut calls the callback of the caller, with the name split on dots (for example `["dmut", "name", "other-name"]`). The callback returns a string, or nothing for unset. Without a callback, the name is unset.
4. At the start of the transaction, dmut sets every name used by a statement in the set to the empty string, with `set_config(name, '', true)`.
5. Before dmut runs a statement in the up direction, it sets each name that the statement lists, with `set_config(name, value, true)` and the value as a bind parameter. An unset name stays the empty string. After the statement, dmut sets each of its names to the empty string again.
6. Before dmut runs a statement in the down direction, it sets each saved name that the statement lists to the value stored with the statement, as in rule 5. Every other name reads as the empty string. A stored unset reads as the empty string. A down that needs a value must list the name with `saved: true`.
7. A statement reads a setting with `current_setting(name)`. A name that a statement does not list reads as the empty string. A name that no statement lists is unknown to Postgres, and `current_setting(name)` raises an error.
8. The identity of a statement is its text, plus the settings digest. The settings digest is the list of its tracked entries, in order, each as the name and either the hash of its value or a marker for unset, so that dmut can name the setting that changed. The marker differs from the empty string. The hash is SHA-256 of the namespace, the mutation, the position, the name and the value, so that equal values in two statements give different hashes. dmut stores the digest with the statement. It stores a value only for a saved entry (rule 12).
9. When the stored identity of a statement differs from the new one, the statement is the first statement of a changed tail (section 5, rule 4). Adding, removing or reordering an entry changes the identity.
10. A new statement has no stored identity, and runs as a new statement.
11. A run that is caused by a settings change has the `reason` field `setting` and the `cause` field with the name of the setting (section 10).
12. dmut stores the value of a saved entry in `statements.saved`, a map from name to value, with a null for unset. Each time the up of the statement runs, dmut stores the value that the up used, so a saved value always is the one of the last up. An entry that is saved and not tracked is stored in this way, and a change of its value redoes nothing.
13. At load time, dmut warns about an entry with `saved: true` when a segment of its name, compared without case, contains `secret`, `password`, `passwd`, `pwd`, `token`, `credential`, `private`, `auth`, `apikey` or `key`. The warning names the file, the mutation, the position and the setting. It is not an error, and there is no option to silence it.

> 📜 **ADR**: In a test on Postgres 18, a name set with `set_config(..., true)` reads as an empty string after the transaction ends, after `RESET`, after `DISCARD ALL` and after a rollback to an earlier savepoint. Only a name that was never set reads as unset (NULL). dmut cannot make a name unset again in a session. So "unset" and "empty" are the same in SQL, and dmut separates them only in the digest. `RELEASE SAVEPOINT` keeps the value, so a savepoint does not limit the scope of a setting. This is why dmut sets all names to empty first (rule 4) and sets them again to empty after each statement (rule 5).

> 📜 **ADR**: Settings belong to the statement, not to the mutation. A settings change on a whole mutation would reapply it, and reapplying a structural mutation drops its data. With the settings on a statement, a change redoes the tail from that statement, as a change of the text does. The statements before it are not touched.

> 📜 **ADR**: An unset value changes the digest. A deploy job that is missing a variable then redoes the tail with an empty value. This is intended. An author who wants an error writes `nullif(current_setting(name), '')` and `raise exception` in the statement.

> 📜 **ADR**: A down that depends on a setting needs the value that the up used. A down with no value fails at once in the test phase (section 9), so the author finds it before any value changes. `saved` makes the author state that the down depends on the value, and dmut stores it in clear. A secret is never saved.

> 📜 **ADR**: The digest hides a value from a casual reader of the database. It does not protect a weak secret against a targeted guess, because the prefix of rule 8 is public. An author whose value is a weak secret uses `tracked: false`, which stores nothing, and never `saved: true`.

> 🔎 **Assumption**: The option name for the library callback is `Settings`, a field of the apply options, of type `func(path []string) *string`.

## 8. Storage

dmut stores its state in three tables of the schema `__dmut__`:

- `__dmut__.mutations`: one row for each mutation. Columns: `namespace`, `name`, `file`, `revision`, `structural`, `reapply_on`, `ts`. Primary key `(namespace, name)`.
- `__dmut__.statements`: one row for each statement. Columns: `namespace`, `mutation`, `position`, `kind` (`reversible`, `seed` or `check`), `up`, `down`, `digest` (the settings digest and the data hash), `saved` (`jsonb`, the values of the saved settings, or null). Primary key `(namespace, mutation, position)`, with a foreign key to `mutations` and `on delete cascade`.
- `__dmut__.deps`: one row for each entry of `deps`. Columns: `namespace`, `mutation`, `position`, `entry` (name or pattern), `reapply_on` (null when the entry has none), `matched` (`text[]`, section 4 rule 10). Same key and foreign key.

Rules:

1. A tail redo deletes the rows of `statements` with `position >= k` and inserts the new tail. An append only inserts.
2. dmut reads the whole state with one query. The query selects the rows of `mutations` and aggregates the rows of `statements` and of `deps` of each mutation, ordered by `position`.
3. The columns `core`, `glue`, `needs` and `glue_needs` and the type `__dmut__.mutation_statement` are removed.
4. The shape of these tables changes through the mutations of dmut's own `__dmut__` definition. No data is moved from the old columns.

> 📜 **ADR**: Tables replace arrays of a composite type, so that a later change of the state is an ordinary tail mutation (`alter table ... add column`) with constraints, indexes and a foreign key available. In a test on Postgres 18, `ALTER TYPE ... ADD ATTRIBUTE`, `RENAME ATTRIBUTE` and `DROP ATTRIBUTE` on `mutation_statement` also worked while a table column of type `mutation_statement[]` held data, so a type change is not impossible. The reason for tables is the nicer tools, not a restriction of the type. In a test, one query that aggregates the two child tables with `jsonb_agg(... order by position)` returned one row for each mutation. Reading it costs a JSON decode.

## 9. Test phase

The test phase runs when anything changed, inside the apply transaction, behind savepoints, and is rolled back. Each step that fails names the mutation and the statement. A down in the test phase receives the saved values that its up stored, as in a real run (section 7, rule 6).

1. All mutations are up.
2. For each mutation, in dependency order: dmut takes the mutation down in full, with every dependent that would reapply because of it, directly or not. Then it brings them all up again. Then it rolls back to the start of this step.
3. dmut takes all mutations down in reverse dependency order, and brings them all up again. During the up, dmut runs each seed twice in a row. Then it rolls back.

> 📜 **ADR**: Step 2 replaces the simulation of every single-statement edit. A tail that comes down removes a subset of the objects of a full down. If the full down works with the declared dependents, a tail down works with them. Step 3 covers the order of statements inside one mutation.

## 10. Logging

Each `run` record keeps the fields `namespace`, `revision`, `mutation`, `dir`. The field `kind` is removed. A `run` record has no statement counts. The summary records "downing all glue" and "glue back up" are removed.

A record for a reapply or a tail redo has a `reason` field with the name of the trigger (`dep_reapplied`, `dep_appended`, `deps_grow`, `deps_shrink`) or `setting`, and a `cause` field with the name of the dependency or of the setting.

## 11. Documentation and tests

1. Documentation under `docs/content` and `README.md` describe this model. The page "core vs glue" is replaced by pages for structural mutations, dependencies and reapply triggers, seeds, and settings. The examples under `examples/` use the new format. The documents that mention `new_sql` and `new_needs` (keys that the code no longer reads) or `children` are corrected. The embedded `__dmut__` definition and every example and test file use `namespace` and `revision` and the file naming of "File layout".
2. Each rule of sections 1 to 10 has an integration test that runs `apply` against a Postgres container with mutation files written in the test. Load-time refusals are tested with the same entry point.
3. These cases have a test: each trigger fires and does not fire; two entries on one dependency (union); the mode `safe` refuses a reapply of a structural mutation; the mode `force` reapplies dependents with `reapply_on: []`; pattern growth after a new mutation; removal of a mutation with a dependent that dropped it from `deps`; a changed `deps` list with no trigger takes no action; a seed is skipped on down and runs twice in the test phase; a seed before a changed statement does not run again; a swap of two statements redoes the tail from the first swapped position; a setting value changes, and only the tail from its statement is redone; a setting becomes unset; a setting is declared with `tracked: false`; a setting of one statement reads as empty in the next statement; a name that no statement lists raises an error; a down reads a saved name as the value its up used, and every other name as empty; a down that needs an unsaved value fails in the test phase; a saved value is still the old one when the current value has changed, and the tail redo drops the old object; an entry saved and not tracked redoes nothing on a change; a saved entry with a name such as `dmut.env.DB_PASSWORD` gives a warning at load time; equal values in two statements give different digests; the `structural` flag change runs nothing and is refused in the mode `safe`; a statement not on the default list is refused in a default mutation and accepted in a structural one; a transaction-control statement is refused in both; a down uses the new graph for its order; a file `<name>.yml` defines the mutation `<name>`; a document whose first key is `revision` is a revision document, and `revision` in any other position is an error; `revision.yml` defines the mutation `revision`; a mutation named `mode` loads under `mutations:`; two definitions of one name are refused; a check statement passes on `true`, fails on `false`, and fails on a result that is not one boolean; a check runs again after a reapply of its dependency; a seed with `data` runs once for each row, is refused on a row of the wrong length, redoes its tail when `data` changes, and accepts a YAML alias; the callback is asked for a name that is neither `env` nor `values`.
4. `CONTEXT.md` is updated whenever a term of this document changes.

---
icon: material/file-tree
---

# Mutation format

Mutations are defined in yaml files read recursively from the paths dmut is given. Files that do not
end in `.yml` or `.yaml` are ignored.

## A mutation file

A file holds one mutation, and the mutation is named after the file, without its extension:
`api.users.yml` defines the mutation `api.users`. The directory the file sits in means nothing.

```yaml
# api.users.yml
# optional, put this mutation in a namespace
namespace: some-name

# optional, true when the mutation holds data or is slow to redo (default: false)
structural: true

# optional, the mutations this one depends on, by name or by pattern
deps: [api, 'types.*']

# optional, which events reapply the mutation (default: see "Structural and default mutations")
reapply_on: [dep_reapplied, dep_appended]

sql:
  - a statement whose down dmut can infer on its own
  - up: the sql that brings this mutation up
    down: the sql that undoes it
  - seed: reference data, run again every time this part goes up
  - check: select true -- a condition that must hold
```

Any other key is an error, so a typo is found when the files are loaded, before dmut connects to a
database.

A file name may only contain letters, digits, `_`, `-` and dots. A dot is a separator: a mutation
named `a.b.c` depends on `a.b` and on `a` when they exist. See [Naming rules](naming-rules.md).

## A revision document

A yaml document whose **first** key is `revision` is a revision document instead. It lists several
mutations in one place, and is what [`dmut collect`](collect-explode.md) writes:

```yaml
revision: 3
namespace: some-name

# optional, how a run reacts to changes that are not appends (safe, normal or force)
mode: normal

mutations:
  api:
    structural: true
    sql:
      - create schema api
  api.users:
    structural: true
    sql:
      - create table api.users (id serial primary key)
```

Revision documents may also carry `delete:` and `override:`; see [Revisions](../revisions/index.md).
The name of the file means nothing for them, and a file may hold several documents separated by
`---`. A mutation may be called `mode` or `revision` there, since names only appear under
`mutations:` and `override:`. A file named `revision.yml` is an ordinary mutation file defining the
mutation `revision`.

Two definitions of the same mutation in one namespace and revision are an error, and the error names
both files. A file without a `revision` is at the revision after the highest one of its namespace
(or at revision 1 when there is none).

There is no key for roles. Create them in a structural mutation of their own, and list it in `deps`
wherever a grant or a policy needs the role.

## In this section

- **[Structural and default mutations](structural.md)** — what holds data and what can be reapplied
  freely.
- **[Statements, seeds and checks](statements.md)** — the forms of a statement, data for seeds,
  assertions.
- **[Dependencies and reapply triggers](dependencies.md)** — `deps`, patterns, and what makes a
  mutation reapply.
- **[Settings](settings.md)** — environment variables and values handed to statements.
- **[Automatic down statements](auto-down.md)** — what dmut can infer, what it can't, and the
  handful of statements where you must write an explicit down.
- **[Naming rules](naming-rules.md)** — dotted names and automatic dependencies.
- **[Namespaces](namespaces.md)** — isolating independent sets of mutations on the same database.
- **[Collecting and exploding](collect-explode.md)** — `dmut collect` / `dmut explode`, and when
  each is useful.

## Changes

Dmut compares each mutation with the version it recorded, statement by statement and in order. A
statement is different when its text changes (whitespace and comments are normalised away, [doc comments](doc-comments.md) included), when its
kind changes, when the `down` of an `up`/`down` pair changes, when the `data` of a seed changes, or
when the value of one of its [tracked settings](settings.md) changes.

- Statements added at the end are an *append*: only the new ones run, nothing comes down.
- From the first statement that differs, that statement and all that follow are the *tail*. The
  recorded tail comes down and the new one goes up. The statements before it are not touched.
- Reordering statements is a change, starting at the first position that holds a different one.
  Two orders that both run can build different databases (columns in a different order, a grant
  followed by a revoke), so dmut does not treat a reorder as "no change".

*BEWARE*: when a statement that creates a table is in the tail, the table is dropped and its data is
lost. This is mostly useful in dev where you can change whatever you want and don't mind destroying
stuff. In production, append, or use [revisions](../revisions/index.md).

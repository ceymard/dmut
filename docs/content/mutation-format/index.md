---
icon: material/file-tree
---

# Mutation format

Mutations are defined in yaml files read recursively from the paths dmut is given.

```yaml
# optional, make all mutations in this file part of a revision
__revision: 1
# optional, make all mutations in this file part of a namespace
__namespace: some-name

mutation_name:
  # optional, names of the mutations whose `core` must run before this one
  needs: [optional, parent, mutation, names]

  core:
    - a statement whose down dmut can infer on its own
    - up: the sql that brings this mutation up
      down: the sql that undoes it

  # optional, mutations that belong to this one.
  # `child_name` below is really the mutation `mutation_name.child_name`, and as such
  # it automatically depends on `mutation_name` (see "Naming rules and children").
  children:
    child_name:
      core:
        - alter table ... add ...

  # optional, names of the mutations whose `glue` must run before this one.
  # There is no need to name mutations whose *core* must run before: all `core` always runs
  # before any `glue`.
  glue_needs: [mutation, names]

  # Lightweight statements, same syntax as `core`
  glue:
    - grant select on table ... to ...

  # optional, only meaningful in a file that sets `__revision`: replaces `needs` in what gets stored
  new_needs: [new, parents]

  # optional, only meaningful in a file that sets `__revision`: replaces `core` in what gets stored
  new_sql:
    - statement that replaces what is in `core`
```

There is no key for roles. Create them in a `core` block of their own mutation — since all `core`
runs before any `glue`, they will exist by the time grants and policies reference them:

```yaml
roles:
  core:
    - create role "@admin";
    - create role "@active";
```

Keys other than the ones above are rejected with an error.

## In this section

- **[core vs glue](core-vs-glue.md)** — why the split exists and what belongs in each.
- **[Automatic down statements](auto-down.md)** — what dmut can infer, what it can't, and the
  handful of statements where you must write an explicit down.
- **[Naming rules and children](naming-rules.md)** — dotted names, automatic dependencies, and
  when to reach for `children` instead of a flat name.
- **[Namespaces](namespaces.md)** — isolating independent sets of mutations on the same database.
- **[Collecting and exploding](collect-explode.md)** — `dmut collect` / `dmut explode`, and when
  each is useful.

## Changes

A mutation is considered different when its `name` or the content of its `core` or `glue` changes.
The `down` half of a statement counts too: editing only an explicit `down:` will trigger a
de-apply.

Comments and whitespace are normalised away before hashing, so reformatting or commenting your SQL
does not cause anything to be re-applied.

When a mutation changes, it and its dependents are downed before being re-applied. *BEWARE*: loss
of data can happen then, as `CREATE TABLE` mutations that change get `DROP`ped. This is mostly
useful in dev where you can change whatever you want and don't mind destroying stuff. In
production, use [revisions](../revisions/index.md).

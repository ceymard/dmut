# Collecting and exploding

## Collecting

`dmut collect <outfile> <paths...>` reads everything dmut would read and writes it back out as a
single yaml file — one document per namespace and revision, mutations sorted by name. Pass `-` as
the outfile to write to stdout.

```sh
dmut collect schema.yml ./mutations
```

The output is meant to be read back by dmut, and is normalised on the way out: statements whose
down can be inferred are written as plain strings, the rest as `up`/`down` pairs, `children:` are
flattened to their real `parent.child` names, and the dependencies dmut derives from dotted names
are left out since it derives them again on load. Collecting an already collected file changes
nothing.

Collecting never changes a mutation's hash, so pointing dmut at a collected file instead of your
tree applies as a no-op to an existing database.

Dmut's own `__dmut__` mutations are left out — they ship inside the binary.

For example, `dmut collect - ./examples/revisions` — the [revisions worked example](../revisions/worked-example.md) — flattens `children: { description_search: ... }` in
`examples/revisions/r2.yml` down to the real mutation name `properties.description_search`, right
next to its parent, one document per revision:

```yaml
__namespace: revisions
__revision: 2

properties:
  core:
    - |-
      create table properties (
        id serial primary key,
        name text not null,
        description text
      );
    - up: |-
        insert into properties (name, description) values
          ('Seaside Inn', 'A quiet hotel by the sea with a pool and lovely staff.'),
          ('Mountain Lodge', 'A cozy retreat with mountain views and a fireplace.');
      down: |-
        delete from properties where name in ('Seaside Inn', 'Mountain Lodge');
  new_sql:
    - |-
      create table properties (
        id serial primary key,
        name text not null,
        description text,
        description_search tsvector
          generated always as (to_tsvector('english', coalesce(description, ''))) stored
      );
    - |-
      create index properties_description_search_idx on properties using gin (description_search);

properties.description_search:
  core:
    - |-
      alter table properties add column description_search tsvector
        generated always as (to_tsvector('english', coalesce(description, ''))) stored;
    - |-
      create index properties_description_search_idx on properties using gin (description_search);
  new_sql: []
```

## Exploding

`dmut explode` is not the inverse of `collect`. It is there to lay the *current* revision out as
neat per-mutation files — the version of the code you are actually working on — and it should be
run on that revision alone. It splits with a regex and knows nothing about documents, so given
several revisions at once, mutations that share a name across them overwrite each other silently.

```sh
dmut explode --out-dir ./out ./schema.yml
```

`__namespace` is written at the top of every file it produces, since each of them is read back as a
set of its own. `__revision` is deliberately not: a file that sets no revision is the current one by
definition, which is exactly what exploded code is. A file declaring more than one namespace is
refused rather than split across silos.

Most files in [`examples/hotel`](https://github.com/ceymard/dmut/tree/main/examples/hotel) hold one
mutation, named after the mutation's last dotted component (`chains.yml` holds `hotel.chains`,
`rooms.yml` holds `hotel.rooms`) — flatter than what `dmut explode` would actually produce for a
dotted name like `hotel.chains` (it would nest that under a `hotel/` directory), but a deliberate,
readable middle ground for a schema that dots nearly everything under one schema-naming mutation.
`hotel.yml` is the one file that intentionally breaks the one-mutation-per-file pattern entirely —
see [Naming rules and children](naming-rules.md#a-convention-name-every-schema-object-schemaobject).

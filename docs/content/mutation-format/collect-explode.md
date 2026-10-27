# Collecting and exploding

## Collecting

`dmut collect <outfile> <paths...>` reads everything dmut would read and writes it back out as a
single yaml file — one revision document per namespace and revision, mutations sorted by name. Pass
`-` as the outfile to write to stdout.

```sh
dmut collect schema.yml ./mutations
```

The output is meant to be read back by dmut, and is normalised on the way out: statements whose
down can be inferred are written as plain strings, the rest as `up`/`down` pairs, `seed` and
`check` statements keep their prefix, and the dependencies dmut derives from dotted names are left
out since it derives them again on load. Collecting an already collected file changes nothing.

Dmut's own `__dmut__` mutations are left out — they ship inside the binary.

For example, `dmut collect - ./examples/revisions` writes one document per revision of the
[revisions worked example](../revisions/worked-example.md). The second one carries the `override:`
and `delete:` of the squash:

```yaml
revision: 2
namespace: revisions
delete: [properties.description_search]

override:
  properties:
    structural: true
    sql:
      - |-
        create table properties (
          id serial primary key,
          name text not null,
          description text,
          description_search tsvector
            generated always as (to_tsvector('english', coalesce(description, ''))) stored
        )
      # ...

mutations:
  properties:
    structural: true
    sql:
      # ...
  properties.description_search:
    structural: true
    sql:
      # ...
```

## Revision shells

`dmut collect -r <n> <outfile> <paths...>` writes a *revision shell* instead: a document at revision
`n` with `mode`, `delete` and `override` present and the current effective definition of every
namespace under `mutations:`, ready to be consolidated by hand. See [Revisions](../revisions/index.md).

## Exploding

`dmut explode --out-dir <dir> <paths...>` writes one file per mutation, named `<mutation>.yml`, from
the current definition of each namespace: the highest revision, with its `override:` entries in
place of the mutations they replace and its `delete:` mutations left out. Each file carries its
`namespace` and no revision, since a file without a revision is the current one by definition.

```sh
dmut explode --out-dir ./out ./schema.yml
```

It is the way back from a collected file to files you can work on: `dmut collect` then `dmut explode`
gives a tree of single-mutation files. The files of [`examples/hotel`](
https://github.com/ceymard/dmut/tree/main/examples/hotel) are laid out that way.

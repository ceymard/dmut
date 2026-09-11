# Namespaces

Mutations can be namespaced by setting `__namespace: <string>` at the toplevel of their file.

They act as silos; namespaced mutations will not touch mutations from other namespaces, and may be
applied completely independently.

`needs` and `glue_needs` cannot cross a namespace — naming a mutation from another namespace is a
load-time error. Dmut cannot see through raw SQL, though, so make absolutely sure that no code from
a namespace references objects created in another. Namespaces are explicitly made for completely
independent code and structures that live in the same database but never interact.

This isolation is enforced for declared dependencies, not for what the SQL itself does. Dmut does
not parse your statements' bodies looking for cross-namespace references — that would mean
building something close to a full SQL compiler, which is out of scope for what dmut is trying to
be. What dmut does instead is catch *some* of these mistakes indirectly: the test phase downs and
re-ups mutations independently, so a namespace that quietly depends on another namespace's object
existing will tend to fail its test once the dependency it's leaning on isn't there — but this is a
side effect of testing, not a guarantee, and plenty of undeclared cross-namespace coupling can
still slip through undetected. Keeping namespaces to genuinely independent code, as intended, is
what actually keeps this safe.

## In this documentation

Every example uses its own namespace:

- [`examples/hotel`](https://github.com/ceymard/dmut/tree/main/examples/hotel), the
  [example database](../example-database/index.md), is namespace `hotel`.
- [`examples/revisions`](https://github.com/ceymard/dmut/tree/main/examples/revisions), the
  [revisions worked example](../revisions/worked-example.md), is namespace `revisions`.

They can be applied to the same database without interacting at all:

```sh
dmut apply postgres://user:pass@localhost/mydb ./examples/hotel ./examples/revisions
dmut down postgres://user:pass@localhost/mydb hotel
dmut down postgres://user:pass@localhost/mydb revisions
```

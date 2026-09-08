# Schema reference

The 25 mutations in [`examples/hotel`](https://github.com/ceymard/dmut/tree/main/examples/hotel),
sorted by name — this is the actual `needs:` output of `dmut collect - ./examples/hotel`, trimmed
to just the dependency edges:

```
extensions:
hotel:
hotel.amenities:
hotel.booking_guests:
  needs: [hotel.bookings, hotel.guests]
hotel.bookings:
  needs: [hotel.guests, hotel.rooms, extensions, hotel.types.booking_status]
hotel.chains:
hotel.guests:
  needs: [hotel.loyalty_tiers, hotel.types.address]
hotel.loyalty_tiers:
hotel.payment_methods:
  needs: [hotel.guests, hotel.types.card_summary]
hotel.payments:
  needs: [hotel.bookings, hotel.payment_methods, hotel.types.payment_status]
hotel.properties:
  needs: [hotel.chains]
hotel.properties.description_search:
hotel.property_amenities:
  needs: [hotel.properties, hotel.amenities]
hotel.rate_plans:
  needs: [hotel.properties]
hotel.reviews:
  needs: [hotel.guests, hotel.properties, hotel.bookings]
hotel.reviews.comment_search:
hotel.room_types:
  needs: [hotel.properties]
hotel.rooms:
  needs: [hotel.room_types, hotel.types.room_status]
hotel.staff:
  needs: [hotel.properties]
hotel.types.address:
hotel.types.booking_status:
hotel.types.card_summary:
hotel.types.payment_status:
hotel.types.room_status:
roles:
```

Every mutation whose `needs:` line is blank still depends on `hotel` in practice — `hotel.amenities`,
`hotel.chains`, `hotel.loyalty_tiers`, `hotel.properties.description_search`,
`hotel.reviews.comment_search` and every `hotel.types.*` all pick that up automatically from being
dotted underneath it. `collect`'s output deliberately omits dependencies it derives from dotted
names (it re-derives them on load) — see [Naming rules and children](../mutation-format/naming-rules.md).
`roles` and `extensions` are the two mutations genuinely independent of `hotel`: neither creates
anything that actually lives inside the schema.

## Table

| Mutation | File | What it covers |
|---|---|---|
| `hotel` / `hotel.types.*` | `hotel.yml` | `create schema hotel`, two composite types (`address`, `card_summary`) and three enums (`room_status`, `booking_status`, `payment_status`) — flat, dotted, no grouping mutation — plus, in `hotel`'s own `meta`, the blanket grants to `~anonymous` (see [sql vs meta](../mutation-format/sql-vs-meta.md#meta-doesnt-always-rerun-when-youd-expect)) |
| `extensions` | `extensions.yml` | `btree_gist`, needed by `hotel.bookings`' GiST exclusion constraint — not dotted under `hotel`, since an extension isn't scoped to one schema |
| `roles` | `roles.yml` | The `~anonymous` role — not dotted under `hotel` either, since a role is cluster-wide |
| `hotel.chains` | `chains.yml` | Hotel groups |
| `hotel.loyalty_tiers` | `loyalty_tiers.yml` | A lookup table, deliberately not an enum |
| `hotel.amenities` | `amenities.yml` | A catalog of amenities |
| `hotel.properties` / `hotel.properties.description_search` | `properties.yml` | Hotel properties, plus `search_properties(query)`, a table-valued/`SETOF` function; the generated FTS column is a `children` example |
| `hotel.room_types` | `room_types.yml` | Categories of room per property |
| `hotel.rooms` | `rooms.yml` | Physical rooms; `room_effective_rate` (one-hop computed column) and `concat_features` (a `VARIADIC` function) |
| `hotel.property_amenities` | `property_amenities.yml` | Many-to-many: properties ↔ amenities |
| `hotel.guests` | `guests.yml` | People who can book; `guest_full_name` (pure computed column) and `split_name` (`OUT`-parameter function — see [Automatic down statements](../mutation-format/auto-down.md)) |
| `hotel.payment_methods` | `payment_methods.yml` | Saved payment methods (composite-typed `card` column) |
| `hotel.bookings` | `bookings.yml` | Reservations, with a GiST exclusion constraint preventing overlapping bookings on the same room; `booking_nights` and `rooms_available` (a default-argument function) |
| `hotel.booking_guests` | `booking_guests.yml` | Many-to-many: bookings ↔ every guest actually staying |
| `hotel.payments` | `payments.yml` | Charges/refunds; `booking_total_paid` (a `STABLE` aggregate-reading computed column) and `booking_stats` (`RETURNS TABLE`) |
| `hotel.reviews` / `hotel.reviews.comment_search` | `reviews.yml` | Guest reviews, a second independent FTS corpus, and `property_average_rating` |
| `hotel.rate_plans` | `rate_plans.yml` | Named rate/cancellation policies |
| `hotel.staff` | `staff.yml` | Property staff, self-referencing via `manager_id` |

See the [walkthrough](walkthrough.md) for how these mutations are actually structured, and
[Naming rules and children](../mutation-format/naming-rules.md) for the `hotel.<object>` naming
convention and what the dotted `hotel.types.*` / `hotel.properties.description_search` names are
doing.

# Schema reference

The 32 mutations in [`examples/hotel`](https://github.com/ceymard/dmut/tree/main/examples/hotel),
sorted by name. Each one is a file named after it (`hotel.rooms.yml` holds `hotel.rooms`). This is
the dependency part of `dmut collect - ./examples/hotel`; every mutation not shown with `deps`
depends on nothing but its dotted parents. `S` marks a [structural](
../mutation-format/structural.md) mutation, the others are default mutations.

```
extensions:                      S
hotel:                           S
hotel.amenities:                 S
hotel.booking_guests:            S  deps: [hotel.bookings, hotel.guests]
hotel.bookings:                  S  deps: [hotel.guests, hotel.rooms, extensions, hotel.types.booking_status]
hotel.bookings.functions:           deps: [hotel.rooms]
hotel.chains:                    S
hotel.grants:                       deps: [roles, hotel.**]
hotel.guests:                    S  deps: [hotel.loyalty_tiers, hotel.types.address]
hotel.guests.functions:
hotel.loyalty_tiers:             S
hotel.payment_methods:           S  deps: [hotel.guests, hotel.types.card_summary]
hotel.payments:                  S  deps: [hotel.bookings, hotel.payment_methods, hotel.types.payment_status]
hotel.payments.functions:           deps: [hotel.bookings, hotel.rooms]
hotel.properties:                S  deps: [hotel.chains]
hotel.properties.description_search: S
hotel.properties.functions:         deps: [hotel.properties.description_search]
hotel.property_amenities:        S  deps: [hotel.properties, hotel.amenities]
hotel.rate_plans:                S  deps: [hotel.properties]
hotel.reviews:                   S  deps: [hotel.guests, hotel.properties, hotel.bookings]
hotel.reviews.comment_search:    S
hotel.reviews.functions:            deps: [hotel.properties]
hotel.room_types:                S  deps: [hotel.properties]
hotel.rooms:                     S  deps: [hotel.properties, hotel.room_types, hotel.types.room_status]
hotel.rooms.functions:              deps: [hotel.room_types]
hotel.staff:                     S  deps: [hotel.properties]
hotel.types.address:             S
hotel.types.booking_status:      S
hotel.types.card_summary:        S
hotel.types.payment_status:      S
hotel.types.room_status:         S
roles:                           S
```

`collect` leaves out the dependencies it derives from dotted names, since it derives them again on
load: every `hotel.*` mutation depends on `hotel`, and each `*.functions` or `*.comment_search`
mutation on the table it is named after. `roles` and `extensions` are the two mutations genuinely
independent of `hotel`: neither creates anything that actually lives inside the schema.

## Table

| Mutation | What it covers |
|---|---|
| `hotel` and `hotel.types.*` | `create schema hotel`, two composite types (`address`, `card_summary`) and three enums (`room_status`, `booking_status`, `payment_status`), one mutation each |
| `extensions` | `btree_gist`, required by `hotel.bookings`' GiST exclusion constraint — not dotted under `hotel`, since an extension isn't scoped to one schema |
| `roles` | The `~anonymous` role — not dotted under `hotel` either, since a role is cluster-wide |
| `hotel.grants` | The blanket grants to `~anonymous`. It reapplies whenever anything in `hotel.**` is reapplied or gets statements appended (see the [walkthrough](walkthrough.md#grants-that-follow-the-schema)) |
| `hotel.chains` | Hotel groups |
| `hotel.loyalty_tiers` | A lookup table, deliberately not an enum |
| `hotel.amenities` | A catalog of amenities |
| `hotel.properties`, `.description_search`, `.functions` | Hotel properties; the generated full-text-search column and its index as a mutation of its own; `search_properties(query)`, a `SETOF` function |
| `hotel.room_types` | Categories of room per property |
| `hotel.rooms`, `.functions` | Physical rooms; `room_effective_rate` (one-hop computed column) and `concat_features` (a `VARIADIC` function) |
| `hotel.property_amenities` | Many-to-many: properties ↔ amenities |
| `hotel.guests`, `.functions` | People who can book; `guest_full_name` (pure computed column) and `split_name` (`OUT`-parameter function — see [Automatic down statements](../mutation-format/auto-down.md)) |
| `hotel.payment_methods` | Saved payment methods (composite-typed `card` column) |
| `hotel.bookings`, `.functions` | Reservations, with a GiST exclusion constraint preventing overlapping bookings on the same room; `booking_nights` and `rooms_available` (a default-argument function) |
| `hotel.booking_guests` | Many-to-many: bookings ↔ every guest actually staying |
| `hotel.payments`, `.functions` | Charges/refunds; `booking_total_paid` (a `STABLE` aggregate-reading computed column) and `booking_stats` (`RETURNS TABLE`) |
| `hotel.reviews`, `.comment_search`, `.functions` | Guest reviews, a second independent full-text-search corpus, and `property_average_rating` |
| `hotel.rate_plans` | Named rate/cancellation policies |
| `hotel.staff` | Property staff, self-referencing via `manager_id` |

See the [walkthrough](walkthrough.md) for how these mutations are actually structured, and
[Naming rules](../mutation-format/naming-rules.md) for the `hotel.<object>` naming convention.

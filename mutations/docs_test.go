package mutations

import (
	"strings"
	"testing"

	"github.com/ceymard/dmut/v3/mutations/model"
)

// The doc comments of every kind of object end up as comments in the database. Run with
// DMUT_TEST_IMAGE to try another Postgres version.
const docObjects = `structural: true
sql:
  - |
    --? The schema.
    create schema shop
  - |
    --? Orders.
    create table shop.orders (
      --? The key.
      id int primary key,
      /*? Who
          ordered. */
      customer text not null default 'a,b',
      total numeric(10, 2) check (total >= 0),
      --? Positive quantity.
      qty int constraint qty_pos check (qty > 0),
      --? Unique customer and total.
      constraint orders_unique unique (customer, total),
      check (total < 1000000)
    )
  - |
    --? A point.
    create type shop.point as (
      --? Horizontal.
      x int,
      y int
    )
  - |
    --? A colour.
    create type shop.colour as enum ('red', 'green')
  - |
    --? A positive number.
    create domain shop.posint as int check (value > 0)
  - |
    --? Open orders.
    create view shop.open_orders (
      --? The id.
      id, who
    ) as select id, customer from shop.orders
  - |
    --? Cached orders.
    create materialized view shop.cached as select id from shop.orders
  - |
    --? Order numbers.
    create sequence shop.order_no
  - |
    --? Fast lookups.
    create index orders_customer on shop.orders (customer)
  - |
    --? Adds two numbers.
    create function shop.add(a int, b int) returns int language sql as $$ select a + b --? stays a plain comment
    $$
  - |
    --? Does nothing.
    create procedure shop.noop() language sql as $$ select 1 $$
  - |
    --? Binary hash.
    create operator shop.## (leftarg = int, rightarg = int, function = int4pl)
  - up: create function shop.trg() returns trigger language plpgsql as $$ begin return new; end $$
    down: drop function shop.trg()
  - |
    --? Keeps the log clean.
    create trigger tr before insert on shop.orders for each row execute function shop.trg()
  - |
    --? Own rows only.
    create policy own on shop.orders using (true)
  - |
    --? Ignore deletes.
    create rule ignore_delete as on delete to shop.orders do instead nothing
  - |
    --? Our wrapper.
    create foreign data wrapper wrap
  - |
    --? Remote.
    create server remote foreign data wrapper wrap
  - |
    --? Everything.
    create publication pub
  - |
    --? Plain english.
    create text search configuration shop.english2 (copy = english)
  - |
    --? A new column.
    alter table shop.orders add column note text
`

func TestDocCommentsEndUpInTheDatabase(t *testing.T) {
	h := newHarness(t)
	h.mustApply(f("shop", docObjects))

	for sql, want := range map[string]string{
		"select obj_description('shop'::regnamespace, 'pg_namespace')":                                                             "The schema.",
		"select obj_description('shop.orders'::regclass, 'pg_class')":                                                              "Orders.",
		"select col_description('shop.orders'::regclass, 1)":                                                                       "The key.",
		"select col_description('shop.orders'::regclass, 2)":                                                                       "Who\nordered.",
		"select col_description('shop.orders'::regclass, 4)":                                                                       "Positive quantity.",
		"select col_description('shop.orders'::regclass, 5)":                                                                       "A new column.",
		"select obj_description(oid, 'pg_constraint') from pg_constraint where conname = 'orders_unique'":                          "Unique customer and total.",
		"select obj_description('shop.point'::regtype::oid, 'pg_type')":                                                            "A point.",
		"select col_description('shop.point'::regtype::text::regclass, 1)":                                                         "Horizontal.",
		"select obj_description('shop.colour'::regtype::oid, 'pg_type')":                                                           "A colour.",
		"select obj_description('shop.posint'::regtype::oid, 'pg_type')":                                                           "A positive number.",
		"select obj_description('shop.open_orders'::regclass, 'pg_class')":                                                         "Open orders.",
		"select col_description('shop.open_orders'::regclass, 1)":                                                                  "The id.",
		"select obj_description('shop.cached'::regclass, 'pg_class')":                                                              "Cached orders.",
		"select obj_description('shop.order_no'::regclass, 'pg_class')":                                                            "Order numbers.",
		"select obj_description('shop.orders_customer'::regclass, 'pg_class')":                                                     "Fast lookups.",
		"select obj_description('shop.add(int,int)'::regprocedure, 'pg_proc')":                                                     "Adds two numbers.",
		"select obj_description('shop.noop()'::regprocedure, 'pg_proc')":                                                           "Does nothing.",
		"select obj_description(oid, 'pg_operator') from pg_operator where oprname = '##' and oprnamespace = 'shop'::regnamespace": "Binary hash.",
		"select obj_description(oid, 'pg_trigger') from pg_trigger where tgname = 'tr'":                                            "Keeps the log clean.",
		"select obj_description(oid, 'pg_policy') from pg_policy where polname = 'own'":                                            "Own rows only.",
		"select obj_description(oid, 'pg_rewrite') from pg_rewrite where rulename = 'ignore_delete'":                               "Ignore deletes.",
		"select obj_description(oid, 'pg_foreign_data_wrapper') from pg_foreign_data_wrapper where fdwname = 'wrap'":               "Our wrapper.",
		"select obj_description(oid, 'pg_foreign_server') from pg_foreign_server where srvname = 'remote'":                         "Remote.",
		"select obj_description(oid, 'pg_publication') from pg_publication where pubname = 'pub'":                                  "Everything.",
		"select obj_description(oid, 'pg_ts_config') from pg_ts_config where cfgname = 'english2'":                                 "Plain english.",
	} {
		h.want(sql, want)
	}

	// A new text only reruns the comments, even where the run mode is safe.
	edited := strings.Replace(docObjects, "Orders.", "All the orders.", 1)
	h.mustApply(f("shop", edited), mode(model.ModeSafe))
	h.wantRan("down:shop up:shop")
	h.want("select obj_description('shop.orders'::regclass, 'pg_class')", "All the orders.")

	// A change to the create itself is a real change, and safe mode says no.
	if err := h.apply(f("shop", strings.Replace(docObjects, "create sequence shop.order_no", "create sequence shop.order_no2", 1)), mode(model.ModeSafe)); err == nil {
		t.Error("changing a create should be refused in safe mode")
	}
}

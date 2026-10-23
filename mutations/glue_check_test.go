package mutations

import (
	"strings"
	"testing"
)

func TestGlueWhitelist(t *testing.T) {
	allowed := []string{
		"create function f() returns int language sql as $$ select 1; delete from t; $$;",
		"create or replace function f() returns int language sql as 'select 1'",
		"create procedure p() language sql as $$ insert into t values (1) $$",
		"create aggregate a(int) (sfunc = f, stype = int)",
		"create view v as select 1",
		"create or replace temp recursive view v as select 1",
		"create trigger tg before insert on t for each row execute function f()",
		"create or replace constraint trigger tg after insert on t execute function f()",
		"create policy p on t using (true)",
		"create rule r as on insert to t do instead nothing",
		"drop function f()",
		"drop view if exists v",
		"drop trigger tg on t",
		"alter function f() owner to bob",
		"alter view v owner to bob",
		"alter policy p on t rename to q",
		"alter default privileges in schema s grant select on tables to bob",
		"alter table t enable row level security",
		"alter table if exists only s.t force row level security",
		"alter table t no force row level security",
		"alter table t disable trigger tg",
		"alter table t enable replica trigger tg",
		"alter table t alter column c set default f(1, 2)",
		"alter table t alter c drop default",
		"alter table t enable row level security, alter column c set default array[1,2], disable trigger all",
		"grant select on t to bob",
		"revoke select on t from bob",
		"comment on table t is 'drop table x; delete from y'",
		"-- a comment\n/* another */ grant select on t to bob;",
		"grant select on t to bob; revoke select on t from alice;",
	}
	for _, sql := range allowed {
		if err := checkGlueSQL(sql); err != nil {
			t.Errorf("%q should be allowed in glue, got: %v", sql, err)
		}
	}

	forbidden := []string{
		"create table t (id int)",
		"create materialized view m as select 1",
		"create unique index i on t (a)",
		"create index i on t (a)",
		"create sequence s",
		"create schema s",
		"create type ty as enum ('a')",
		"create extension e",
		"create role r",
		"create schema s create view v as select 1",
		"drop table t",
		"drop materialized view m",
		"drop schema s",
		"alter table t add column c int",
		"alter table t add constraint k check (true)",
		"alter table t rename to u",
		"alter table t owner to bob",
		"alter table t alter column c type text",
		"alter table t alter column c set not null",
		"alter table t alter constraint k deferrable",
		"alter table t enable row level security, add column c int",
		"alter table t enable row level security, drop column c",
		"alter table t alter column c set default 1, add column d int",
		"alter table all in tablespace x set tablespace y",
		"alter materialized view m rename to n",
		"alter sequence s restart",
		"alter type ty add value 'b'",
		"alter system set work_mem = 1",
		"select 1",
		"select * into t2 from t",
		"with x as (delete from t returning *) select * from x",
		"explain analyze insert into t values (1)",
		"insert into t values (1)",
		"update t set a = 1",
		"delete from t",
		"truncate t",
		"merge into t using s on true when matched then delete",
		"copy t from stdin",
		"do $$ begin delete from t; end $$",
		"call p()",
		"set search_path = x",
		"reset all",
		"refresh materialized view m",
		"reindex table t",
		"cluster t",
		"vacuum t",
		"lock table t",
		"import foreign schema s from server x into y",
		"reassign owned by a to b",
		"drop owned by a",
		"begin",
		// A second statement after an allowed one is checked too.
		"grant select on t to bob; delete from t",
		"grant select on t to bob; delete from t;",
		// A semicolon in a string or a comment does not hide what follows.
		"comment on table t is 'a;'; delete from t",
		"grant select on t to bob; -- ;\n delete from t",
	}
	for _, sql := range forbidden {
		if err := checkGlueSQL(sql); err == nil {
			t.Errorf("%q should be refused in glue", sql)
		}
	}
}

func TestLoaderRefusesForbiddenGlue(t *testing.T) {
	refuses(t, "not allowed in glue", map[string]string{
		"a.yml": "x:\n  core:\n    - create table t (id int);\n  glue:\n    - create index i on t (id);\n",
	})
}

func TestLoaderRefusesForbiddenHandWrittenGlueDown(t *testing.T) {
	refuses(t, "not allowed in glue", map[string]string{
		"a.yml": "x:\n  glue:\n    - up: create view v as select 1;\n      down: delete from audit;\n",
	})
}

func TestLoaderRefusesForbiddenGlueUp(t *testing.T) {
	refuses(t, "not allowed in glue", map[string]string{
		"a.yml": "x:\n  glue:\n    - up: insert into t values (1);\n      down: delete from t;\n",
	})
}

func TestLoaderErrorNamesTheStatement(t *testing.T) {
	_, err := load(t, map[string]string{
		"a.yml": "x:\n  glue:\n    - grant select on t to bob;\n    - up: drop table t;\n      down: grant select on t to bob;\n",
	})
	if err == nil {
		t.Fatal("expected a refusal")
	}
	for _, want := range []string{"drop table t", "move it to core"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q, got: %v", want, err)
		}
	}
}

func TestLoaderAcceptsAnythingInCore(t *testing.T) {
	mustLoad(t, map[string]string{
		"a.yml": "x:\n  core:\n    - create table t (id int);\n    - create materialized view m as select 1;\n    - create index i on t (id);\n",
	})
}

func TestLoaderAcceptsAllowedGlue(t *testing.T) {
	mustLoad(t, map[string]string{
		"a.yml": "x:\n  core:\n    - create table t (id int);\n  glue:\n    - alter table t enable row level security;\n    - create view v as select 1;\n    - up: create function f() returns int language sql as $$ select 1; delete from t; $$;\n      down: drop function f();\n",
	})
}

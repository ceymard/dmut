package mutations

import (
	"strings"
	"testing"
)

// readLog writes the value of each name into the table log, one row per name.
func readLog(names ...string) string {
	var b strings.Builder
	b.WriteString("deps: [log]\nsql:\n")
	for _, name := range names {
		b.WriteString("  - seed: insert into log values (current_setting('" + name + "'))\n    settings: [" + name + "]\n")
	}
	return b.String()
}

func logged(h *harness) string { return h.query("select string_agg(msg, ',' order by ctid) from log") }

func TestSettingsFromEnvironmentValuesAndCallback(t *testing.T) {
	t.Setenv("DMUT_TEST_ENV", "from-env")
	h := newHarness(t)
	h.mustApply(f("log", logTable, "r", readLog("dmut.env.DMUT_TEST_ENV", "dmut.values.name", "dmut.app.color", "dmut.env.DMUT_TEST_MISSING")),
		func(o *Options) {
			o.Values = map[string]string{"name": "val"}
			o.Settings = func(path []string) *string {
				if strings.Join(path, ".") == "dmut.app.color" {
					v := "blue"
					return &v
				}
				return nil
			}
		})
	// An unset name reads as an empty string.
	if got := logged(h); got != "from-env,val,blue," {
		t.Errorf("got %q", got)
	}
}

func TestSettingsAreScopedToTheirStatement(t *testing.T) {
	h := newHarness(t)
	h.mustApply(f("log", logTable, "r", `deps: [log]
sql:
  - seed: insert into log values (current_setting('dmut.values.a'))
    settings: [dmut.values.a]
  - seed: insert into log values (current_setting('dmut.values.a'))
`), func(o *Options) { o.Values = map[string]string{"a": "x"} })
	// The second statement does not list the name, and sees it empty - it is known, since another statement lists it.
	if got := logged(h); got != "x," {
		t.Errorf("got %q", got)
	}
}

func TestSettingNobodyListsIsAnError(t *testing.T) {
	h := newHarness(t)
	err := h.apply(f("log", logTable, "r", "deps: [log]\nsql:\n  - seed: insert into log values (current_setting('dmut.values.nobody'))\n"))
	if err == nil || !strings.Contains(err.Error(), "dmut.values.nobody") {
		t.Errorf("want Postgres to refuse the unknown name, got %v", err)
	}
}

func TestSettingChangeRedoesOnlyTheTailFromItsStatement(t *testing.T) {
	h := newHarness(t)
	files := f("log", logTable, "r", `deps: [log]
sql:
  - seed: insert into log values ('one')
  - seed: insert into log values (current_setting('dmut.values.a'))
    settings: [dmut.values.a]
  - seed: insert into log values ('three')
`)
	set := func(v string) func(*Options) {
		return func(o *Options) { o.Values = map[string]string{"a": v} }
	}
	h.mustApply(files, set("1"))
	h.mustApply(files, set("1"))
	h.wantRan("") // same value: nothing happens
	h.mustApply(files, set("2"))
	h.wantRan("down:r(setting:dmut.values.a) up:r(setting:dmut.values.a)")
	if got := logged(h); got != "one,1,three,2,three" {
		t.Errorf("got %q", got)
	}
}

func TestUntrackedSettingNeverRedoes(t *testing.T) {
	h := newHarness(t)
	files := f("log", logTable, "r", `deps: [log]
sql:
  - seed: insert into log values (current_setting('dmut.values.a'))
    settings: [{name: dmut.values.a, tracked: false}]
`)
	h.mustApply(files, func(o *Options) { o.Values = map[string]string{"a": "1"} })
	h.mustApply(files, func(o *Options) { o.Values = map[string]string{"a": "2"} })
	h.wantRan("")
	if got := logged(h); got != "1" {
		t.Errorf("got %q", got)
	}
}

func TestSettingBecomingUnsetRedoes(t *testing.T) {
	h := newHarness(t)
	files := f("log", logTable, "r", readLog("dmut.values.a"))
	h.mustApply(files, func(o *Options) { o.Values = map[string]string{"a": ""} })
	h.mustApply(files) // unset is not the same as empty
	h.wantRan("down:r(setting:dmut.values.a) up:r(setting:dmut.values.a)")
}

func TestSavedSettingReachesTheDown(t *testing.T) {
	h := newHarness(t)
	mk := func(settings string) map[string]string {
		return f("m", `structural: true
sql:
  - up: do $$ begin execute format('create table %I (a int)', current_setting('dmut.values.p') || '_t'); end $$
    down: do $$ begin execute format('drop table %I', current_setting('dmut.values.p') || '_t'); end $$
    settings: [`+settings+`]
`)
	}
	set := func(v string) func(*Options) {
		return func(o *Options) { o.Values = map[string]string{"p": v} }
	}

	// Saved: the down of the old value drops the table the old value created.
	h.mustApply(mk("{name: dmut.values.p, saved: true}"), set("a"))
	h.want("select to_regclass('a_t') is not null", "true")
	h.mustApply(mk("{name: dmut.values.p, saved: true}"), set("b"))
	h.want("select to_regclass('a_t') is null and to_regclass('b_t') is not null", "true")
	h.want("select saved::text from __dmut__.statements where namespace = '' and mutation = 'm'", `{"dmut.values.p": "b"}`)

	// Not saved: the down reads an empty value, and so cannot drop the table.
	h2 := newHarness(t)
	h2.mustApply(mk("dmut.values.p"), set("a"))
	err := h2.apply(mk("dmut.values.p"), set("b"))
	if err == nil || !strings.Contains(err.Error(), "_t") {
		t.Errorf("the down should have read an empty setting and failed, got %v", err)
	}
}

func TestSecretLookingSavedSettingWarns(t *testing.T) {
	h := newHarness(t)
	h.mustApply(f("m", "sql:\n  - seed: select 1\n    settings: [{name: dmut.env.DB_PASSWORD, saved: true}]\n"))
	warned := false
	for _, rec := range *h.rec.out {
		if rec["msg"] != nil && strings.Contains(rec["msg"].(string), "DB_PASSWORD") {
			warned = true
		}
	}
	if !warned {
		t.Errorf("want a warning about the saved secret, got %v", *h.rec.out)
	}
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ceymard/dmut/v2/mutations"
)

// basic is a schema and a table, with no roles: roles are cluster-wide and would collide
// between tests sharing the same postgres.
const basic = `api:
  structural: true
  sql:
    - create schema api

api.users:
  structural: true
  sql:
    - |
      create table api.users (
        id serial primary key,
        name text not null
      )

api.users.grant:
  sql:
    - grant select on table api.users to public
`

// basicFiles is basic as a revision document.
func basicFiles() map[string]string {
	return map[string]string{"a.yml": "revision: 1\nmutations:\n" + indent(basic)}
}

func indent(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		if line == "" {
			b.WriteString("\n")
			continue
		}
		b.WriteString("  " + line + "\n")
	}
	return b.String()
}

// captureLogs routes slog.Default() into a JSON buffer for the rest of the test and returns
// the decoded records on demand.
func captureLogs(t *testing.T) func() []map[string]any {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	return func() []map[string]any {
		t.Helper()
		var records []map[string]any
		for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			var rec map[string]any
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				t.Fatalf("not a JSON line: %q: %v", line, err)
			}
			records = append(records, rec)
		}
		return records
	}
}

func findRecord(records []map[string]any, match func(map[string]any) bool) map[string]any {
	for _, rec := range records {
		if match(rec) {
			return rec
		}
	}
	return nil
}

func hasRecord(records []map[string]any, msg string, icon any) bool {
	return findRecord(records, func(r map[string]any) bool { return r["msg"] == msg && r["icon"] == icon }) != nil
}

func TestApplyCommandFlags(t *testing.T) {
	ctx := context.Background()
	if err := (ApplyCmd{Uri: "x", Force: true, Safe: true}).Run(ctx); err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("-f and -s exclude each other, got %v", err)
	}
	if err := (ApplyCmd{Uri: "x", Set: []string{"novalue"}}).Run(ctx); err == nil || !strings.Contains(err.Error(), "N=value") {
		t.Errorf("--set wants N=value, got %v", err)
	}
}

func TestApplyCommandSetsValues(t *testing.T) {
	uri := testdb(t)
	dir := mutationDir(t, map[string]string{"a.yml": `
structural: true
sql:
  - create table t (v text)
  - seed: insert into t values (current_setting('dmut.values.greeting'))
    settings: [dmut.values.greeting]
`})
	if err := (ApplyCmd{Uri: uri, Paths: []string{dir}, Set: []string{"greeting=hello, world=1"}}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !exists(t, uri, "select 1 from t where v = 'hello, world=1'") {
		t.Error("the value should keep its commas")
	}
}

func TestApplyLogsStructuredAttributes(t *testing.T) {
	records := captureLogs(t)
	uri := testdb(t)
	mustApply(t, uri, mutationDir(t, basicFiles()))
	all := records()

	run := findRecord(all, func(r map[string]any) bool { return r["msg"] == "run" && r["mutation"] == "api.users" })
	if run == nil || run["dir"] != "up" || run["namespace"] != "" || run["revision"] == nil {
		t.Errorf("expected a run record for api.users (default namespace is the empty string, revision included), got %v", run)
	}
	if _, ok := run["kind"]; ok {
		t.Errorf("a run record has no kind: %v", run)
	}
	if findRecord(all, func(r map[string]any) bool { return r["msg"] == "no errors" }) == nil ||
		findRecord(all, func(r map[string]any) bool { return r["msg"] == "committed" }) == nil {
		t.Error("expected the no errors and committed milestones")
	}

	connecting := findRecord(all, func(r map[string]any) bool { return r["msg"] == "connecting" })
	if connecting == nil || !strings.Contains(connecting["uri"].(string), "xxxxx") || strings.Contains(connecting["uri"].(string), "dmut:dmut@") {
		t.Errorf("the connection password must be redacted, got %v", connecting)
	}
	if findRecord(all, func(r map[string]any) bool { return r["msg"] == "sql" && r["level"] == "DEBUG" }) == nil {
		t.Error("every statement is logged at debug level with its sql")
	}
	for _, rec := range all {
		line, _ := json.Marshal(rec)
		if strings.Contains(string(line), `\u001b`) {
			t.Errorf("no ANSI escape may end up in a structured record: %s", line)
		}
		if rec["phase"] == "test" {
			t.Errorf("records of a successful test phase must be dropped: %v", rec)
		}
	}
}

func TestReapplyRecordsCarryReasonAndCause(t *testing.T) {
	records := captureLogs(t)
	uri := testdb(t)
	mustApply(t, uri, mutationDir(t, map[string]string{
		"z.yml": "sql:\n  - seed: select 1\n",
		"f.yml": "deps: [z]\nsql:\n  - create view v as select 1 as x\n",
	}))
	mustApply(t, uri, mutationDir(t, map[string]string{"f.yml": "sql:\n  - create view v as select 1 as x\n"}))
	rec := findRecord(records(), func(r map[string]any) bool {
		return r["msg"] == "run" && r["reason"] == "dep_reapplied" && r["dir"] == "down"
	})
	if rec == nil || rec["cause"] != "z" || rec["mutation"] != "f" {
		t.Errorf("want the reapply of f caused by z, got %v", rec)
	}
}

const brokenSyntax = `structural: true
sql:
  - create schema api
  - up: create tabel api.oops (id int)
    down: drop table api.oops
`

func TestFailedApplyCarriesTheRawPostgresError(t *testing.T) {
	uri := testdb(t)
	err := apply(t, uri, mutationDir(t, map[string]string{"a.yml": brokenSyntax}))
	if err == nil {
		t.Fatal("expected a syntax error")
	}

	got := map[string]any{}
	attrs := mutations.ErrorAttrs(err)
	for i := 0; i < len(attrs); i += 2 {
		got[attrs[i].(string)] = attrs[i+1]
	}
	if got["sqlstate"] != "42601" {
		t.Errorf("expected the syntax error sqlstate, got %v", got["sqlstate"])
	}
	if sql, _ := got["sql"].(string); sql != "create tabel api.oops (id int)" {
		t.Errorf("expected the raw failing statement, got %q", got["sql"])
	}
	if pos, _ := got["position"].(int); pos != 8 {
		t.Errorf("expected the position of 'tabel' (8), got %v", got["position"])
	}
	if got["statement_index"] != 2 {
		t.Errorf("expected statement_index=2, got %v", got["statement_index"])
	}
	if strings.Contains(err.Error(), "\x1b") {
		t.Error("errors must not embed ANSI escapes")
	}

	var buf bytes.Buffer
	flags := LogFlags{Color: "never"}
	flags.reportError(flags.NewLogger(&buf), &buf, err)
	if !strings.Contains(buf.String(), "syntax error") || !strings.Contains(buf.String(), "    create tabel api.oops") {
		t.Errorf("the plain report should show the server message and the sql:\n%s", buf.String())
	}
}

func TestFailedTestPhaseReplaysItsRecords(t *testing.T) {
	records := captureLogs(t)
	uri := testdb(t)
	err := apply(t, uri, mutationDir(t, map[string]string{"a.yml": "structural: true\nsql:\n  - up: create table t (id int)\n    down: drop table does_not_exist\n"}))
	if err == nil {
		t.Fatal("expected the test phase to fail on the broken down")
	}
	if findRecord(records(), func(r map[string]any) bool { return r["phase"] == "test" && r["msg"] == "run" }) == nil {
		t.Errorf("expected the records of the failed test phase to be replayed with phase=test, got %v", records())
	}
}

func TestMilestoneIconsAndRollbacks(t *testing.T) {
	records := captureLogs(t)
	uri := testdb(t)
	mustApply(t, uri, mutationDir(t, basicFiles()))
	all := records()
	for msg, icon := range map[string]string{"performing tests": "🧪", "tests passed": "🆗", "no errors": "🎉", "committed": "✅"} {
		if !hasRecord(all, msg, icon) {
			t.Errorf("expected %q to carry the %s icon", msg, icon)
		}
	}
	if findRecord(all, func(r map[string]any) bool { return r["icon"] == "⛔" }) != nil {
		t.Error("⛔ means a rollback after a failure, not part of a successful run")
	}

	// A dry run rolls back without the stop icon.
	records = captureLogs(t)
	if err := (ApplyCmd{Uri: testdb(t), Paths: []string{mutationDir(t, basicFiles())}, Dry: true}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !hasRecord(records(), "rolling back, nothing to commit", nil) || findRecord(records(), func(r map[string]any) bool { return r["icon"] == "⛔" }) != nil {
		t.Errorf("a dry run is not a failure: %v", records())
	}

	// A failure does.
	records = captureLogs(t)
	if err := apply(t, testdb(t), mutationDir(t, map[string]string{"a.yml": brokenSyntax})); err == nil {
		t.Fatal("expected a failure")
	}
	if !hasRecord(records(), "rolling back", "⛔") {
		t.Errorf("expected the failed run to log ⛔ rolling back, got %v", records())
	}
}

func TestDownCommand(t *testing.T) {
	uri := testdb(t)
	mustApply(t, uri, mutationDir(t, basicFiles()))
	if err := (DownCmd{Uri: uri, Namespace: "", Dry: true}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !schemaExists(t, uri, "api") {
		t.Error("a dry run changes nothing")
	}
	if err := (DownCmd{Uri: uri, Namespace: ""}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if schemaExists(t, uri, "api") || savedMutations(t, uri, "") != 0 {
		t.Error("the namespace should be gone")
	}
}

func TestCollectAndExplodeCommands(t *testing.T) {
	dir := mutationDir(t, basicFiles())
	out := filepath.Join(t.TempDir(), "all.yml")
	if err := (CollectCmd{Outfile: out, Paths: []string{dir}}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	collected, err := os.ReadFile(out)
	if err != nil || !strings.Contains(string(collected), "api.users.grant:") || !strings.Contains(string(collected), "revision: 1") {
		t.Fatalf("unexpected collect: %s %v", collected, err)
	}

	exploded := t.TempDir()
	if err := (ExplodeCmd{OutDir: exploded, Paths: []string{out}}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"api.yml", "api.users.yml", "api.users.grant.yml"} {
		if _, err := os.Stat(filepath.Join(exploded, name)); err != nil {
			t.Errorf("explode should write %s: %v", name, err)
		}
	}
	// The exploded files are a valid project of their own.
	uri := testdb(t)
	mustApply(t, uri, exploded)
	if !tableExists(t, uri, "users") {
		t.Error("the exploded files should apply")
	}
}

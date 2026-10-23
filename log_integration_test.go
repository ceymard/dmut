package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/ceymard/dmut/v2/mutations"
)

// captureLogs routes slog.Default() into a JSON buffer for the rest of the test
// and returns the decoded records on demand.
func captureLogs(t *testing.T) func() []map[string]any {
	t.Helper()
	return captureLogsAt(t, slog.LevelDebug)
}

// captureLogsAt is captureLogs with a minimum level: debug (what -v sets) shows
// every glue step of a rebuild, info (the default) sums them up.
func captureLogsAt(t *testing.T, level slog.Level) func() []map[string]any {
	t.Helper()

	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: level})))
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

func TestApplyLogsStructuredAttributes(t *testing.T) {
	records := captureLogs(t)
	uri := testdb(t)
	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basic}))
	all := records()

	run := findRecord(all, func(r map[string]any) bool { return r["mutation"] == "api.users" && r["kind"] == "core" })
	if run == nil || run["msg"] != "run" || run["dir"] != "up" || run["namespace"] != "" || run["revision"] == nil {
		t.Errorf("expected a run record for api.users core (default namespace is the empty string, revision included), got %v", run)
	}
	if findRecord(all, func(r map[string]any) bool { return r["mutation"] == "api.users" && r["kind"] == "glue" }) == nil {
		t.Error("expected a run record for the api.users glue")
	}
	if findRecord(all, func(r map[string]any) bool { return r["msg"] == "no errors" }) == nil ||
		findRecord(all, func(r map[string]any) bool { return r["msg"] == "committed" }) == nil {
		t.Error("expected the no errors and committed milestones")
	}

	connecting := findRecord(all, func(r map[string]any) bool { return r["msg"] == "connecting" })
	if connecting == nil || !strings.Contains(connecting["uri"].(string), "xxxxx") || strings.Contains(connecting["uri"].(string), "dmut:dmut@") {
		t.Errorf("the connection password must be redacted, got %v", connecting)
	}

	sql := findRecord(all, func(r map[string]any) bool { return r["msg"] == "sql" && r["level"] == "DEBUG" })
	if sql == nil || sql["sql"] == "" {
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

const brokenSyntax = `api:
  core:
    - create schema api;
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
	if strings.Contains(err.Error(), "\x1b") || strings.Contains(got["sql"].(string), "\x1b") {
		t.Error("errors must not embed ANSI escapes")
	}

	var buf bytes.Buffer
	flags := LogFlags{Color: "never"}
	flags.reportError(flags.NewLogger(&buf), &buf, err)
	if !strings.Contains(buf.String(), "syntax error") || !strings.Contains(buf.String(), "    create tabel api.oops") {
		t.Errorf("the plain report should show the server message and the sql:\n%s", buf.String())
	}
}

// The test phase runs silently, and only replays what it logged if it fails.
const failingDown = `api:
  core:
    - create schema api;
api.t:
  core:
    - up: create table api.t (id int);
      down: drop table api.does_not_exist;
`

func TestFailedTestPhaseReplaysItsRecords(t *testing.T) {
	records := captureLogs(t)
	uri := testdb(t)

	err := apply(t, uri, mutationDir(t, map[string]string{"a.yml": failingDown}))
	if err == nil {
		t.Fatal("expected the test phase to fail on the broken down")
	}

	replayed := findRecord(records(), func(r map[string]any) bool { return r["phase"] == "test" })
	if replayed == nil {
		t.Fatalf("expected the records of the failed test phase to be replayed with phase=test, got %v", records())
	}
	if findRecord(records(), func(r map[string]any) bool { return r["phase"] == "test" && r["msg"] == "run" }) == nil {
		t.Error("the replay should include the runs that led to the failure")
	}
}

func hasRecord(records []map[string]any, msg string, icon any) bool {
	return findRecord(records, func(r map[string]any) bool { return r["msg"] == msg && r["icon"] == icon }) != nil
}

func TestMilestoneIcons(t *testing.T) {
	records := captureLogs(t)
	uri := testdb(t)
	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basic}))
	all := records()

	for msg, icon := range map[string]string{"performing tests": "🧪", "tests passed": "🆗", "no errors": "🎉", "committed": "✅"} {
		if !hasRecord(all, msg, icon) {
			t.Errorf("expected %q to carry the %s icon", msg, icon)
		}
	}
	if findRecord(all, func(r map[string]any) bool { return r["icon"] == "⛔" }) != nil {
		t.Error("⛔ means a rollback after a failure, not part of a successful run")
	}
}

func TestDryRunRollbackIsNotFlaggedAsAFailure(t *testing.T) {
	records := captureLogs(t)
	uri := testdb(t)

	if err := (ApplyCmd{Uri: uri, Paths: []string{mutationDir(t, map[string]string{"a.yml": basic})}, Dry: true}).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	all := records()

	if !hasRecord(all, "rolling back, nothing to commit", nil) {
		t.Errorf("a dry run rolls back without an icon, got %v", all)
	}
	if findRecord(all, func(r map[string]any) bool { return r["icon"] == "⛔" }) != nil {
		t.Error("a dry run is not a failure")
	}
}

func TestFailureRollbackCarriesTheStopIcon(t *testing.T) {
	records := captureLogs(t)
	uri := testdb(t)

	if err := apply(t, uri, mutationDir(t, map[string]string{"a.yml": brokenSyntax})); err == nil {
		t.Fatal("expected a failure")
	}
	if !hasRecord(records(), "rolling back", "⛔") {
		t.Errorf("expected the failed run to log ⛔ rolling back, got %v", records())
	}
}

// basicWithColumn changes basic's core in a way that needs a real down; glue is
// the grant given, so a test can also edit the glue.
func basicWithColumn(grant string) string {
	return `api:
  core:
    - create schema api;

api.users:
  core:
    - |
      create table api.users (
        id serial primary key,
        name text not null,
        email text
      );
  glue:
    - ` + grant + `
`
}

// after returns the records from the first one matching match onwards.
func after(records []map[string]any, match func(map[string]any) bool) []map[string]any {
	for i, rec := range records {
		if match(rec) {
			return records[i:]
		}
	}
	return nil
}

func isMsg(msg string) func(map[string]any) bool {
	return func(r map[string]any) bool { return r["msg"] == msg }
}

func countRecords(records []map[string]any, match func(map[string]any) bool) int {
	n := 0
	for _, r := range records {
		if match(r) {
			n++
		}
	}
	return n
}

func isGlueRun(r map[string]any) bool { return r["msg"] == "run" && r["kind"] == "glue" }

func TestGlueRebuildCausedByCoreIsSummedUp(t *testing.T) {
	records := captureLogsAt(t, slog.LevelInfo)
	uri := testdb(t)
	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basic}))
	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basicWithColumn("grant select on table api.users to public;")}))

	second := after(records(), isMsg("downing all glue (core changed)"))
	if second == nil {
		t.Fatal("expected the glue down summary")
	}
	if got := countRecords(second, isGlueRun); got != 0 {
		t.Errorf("the rebuilt glue must not be logged one by one, got %d run records", got)
	}
	down := second[0]
	if down["icon"] != "🔴" || down["mutations"] != float64(1) || down["statements"] != float64(1) {
		t.Errorf("unexpected down summary: %v", down)
	}
	up := findRecord(second, isMsg("glue back up"))
	if up == nil || up["icon"] != "🟢" || up["mutations"] != float64(1) {
		t.Fatalf("unexpected up summary: %v", up)
	}
	if _, ok := up["changed"]; ok {
		t.Errorf("no glue changed on its own, got changed=%v", up["changed"])
	}
	if countRecords(second, isMsg("downing all glue (core changed)")) != 1 || countRecords(second, isMsg("glue back up")) != 1 {
		t.Error("each summary is logged exactly once")
	}
	// The core mutation is still logged, between the two summaries.
	core := findRecord(second, func(r map[string]any) bool { return r["msg"] == "run" && r["kind"] == "core" && r["dir"] == "down" })
	if core == nil || core["icon"] != "🔴" {
		t.Errorf("core runs stay visible and carry the down icon, got %v", core)
	}
}

func TestGlueRebuildNamesTheGlueThatChanged(t *testing.T) {
	records := captureLogsAt(t, slog.LevelInfo)
	uri := testdb(t)
	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basic}))
	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basicWithColumn("grant select, insert on table api.users to public;")}))

	up := findRecord(records(), isMsg("glue back up"))
	if up == nil {
		t.Fatal("expected the glue up summary")
	}
	if changed, _ := up["changed"].([]any); len(changed) != 1 || changed[0] != "api.users" {
		t.Errorf("expected changed=[api.users], got %v", up["changed"])
	}
}

func TestGlueOnlyChangeKeepsItsRunRecords(t *testing.T) {
	records := captureLogsAt(t, slog.LevelInfo)
	uri := testdb(t)
	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basic}))
	changed := `api:
  core:
    - create schema api;

api.users:
  core:
    - |
      create table api.users (
        id serial primary key,
        name text not null
      );
  glue:
    - grant select, insert on table api.users to public;
`
	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": changed}))

	all := records()
	if findRecord(all, isMsg("downing all glue (core changed)")) != nil || findRecord(all, isMsg("glue back up")) != nil {
		t.Error("a glue-only change is not a rebuild: no summaries")
	}
	down := findRecord(all, func(r map[string]any) bool { return isGlueRun(r) && r["dir"] == "down" })
	if down == nil || down["icon"] != "🔴" {
		t.Errorf("expected the glue down run with the down icon, got %v", down)
	}
	if up := findRecord(all[1:], func(r map[string]any) bool { return isGlueRun(r) && r["dir"] == "up" && r["icon"] == "🟢" }); up == nil {
		t.Error("expected glue up runs with the up icon")
	}
}

func TestVerboseKeepsTheGlueRunRecordsOfARebuild(t *testing.T) {
	records := captureLogsAt(t, slog.LevelDebug)
	uri := testdb(t)
	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basic}))
	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basicWithColumn("grant select on table api.users to public;")}))

	second := after(records(), isMsg("downing all glue (core changed)"))
	if second == nil || findRecord(second, isMsg("glue back up")) == nil {
		t.Fatal("the summaries are still logged with -v")
	}
	if got := countRecords(second, isGlueRun); got < 2 {
		t.Errorf("-v shows each glue run, got %d", got)
	}
}

func TestFailedGlueRebuildReplaysWhatRanBeforeTheFailure(t *testing.T) {
	records := captureLogsAt(t, slog.LevelInfo)
	uri := testdb(t)
	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basic}))
	broken := basicWithColumn("up: grant select on table api.nosuch to public;\n      down: revoke select on table api.users from public;")
	if err := apply(t, uri, mutationDir(t, map[string]string{"a.yml": broken})); err == nil {
		t.Fatal("expected the glue up to fail")
	}

	second := after(records(), isMsg("downing all glue (core changed)"))
	if second == nil {
		t.Fatal("expected the glue down summary")
	}
	note := after(second, isMsg("glue steps before the failure:"))
	if note == nil {
		t.Fatalf("expected the replay note, got %v", second)
	}
	if countRecords(note, isGlueRun) != 2 {
		t.Errorf("expected the glue down and the failing glue up to be replayed, got %v", note)
	}
	if findRecord(second, isMsg("glue back up")) != nil {
		t.Error("the glue never came back up")
	}
}

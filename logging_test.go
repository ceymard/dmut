package main

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/samber/oops"
)

const escape = "\x1b["

func TestJSONLoggerIsPlainJSONWithATimestamp(t *testing.T) {
	var buf bytes.Buffer
	// color is requested on purpose: it must have no effect on JSON
	logger := LogFlags{JSON: true, Color: "always"}.NewLogger(&buf)

	logger.Info("🎉 no errors", "namespace", "api", "revision", 3)

	if strings.Contains(buf.String(), escape) {
		t.Fatalf("JSON output must never contain ANSI escapes: %q", buf.String())
	}
	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("expected one JSON object, got %q: %v", buf.String(), err)
	}
	if rec["msg"] != "🎉 no errors" || rec["namespace"] != "api" || rec["revision"] != float64(3) {
		t.Errorf("unexpected record: %v", rec)
	}
	if _, ok := rec["time"]; !ok {
		t.Error("JSON records always carry a time")
	}
	if _, ok := rec["source"]; ok {
		t.Error("no source expected without --source")
	}
}

func TestPlainLoggerHidesTimeUnlessAsked(t *testing.T) {
	timeRe := regexp.MustCompile(`\d\d:\d\d:\d\d`)

	var buf bytes.Buffer
	LogFlags{Color: "never"}.NewLogger(&buf).Info("hello", "namespace", "api")
	if timeRe.MatchString(buf.String()) {
		t.Errorf("no time expected by default: %q", buf.String())
	}
	if !strings.Contains(buf.String(), "hello") || !strings.Contains(buf.String(), "namespace=api") {
		t.Errorf("unexpected plain line: %q", buf.String())
	}

	buf.Reset()
	LogFlags{Color: "never", Time: true}.NewLogger(&buf).Info("hello")
	if !timeRe.MatchString(buf.String()) {
		t.Errorf("--time must show the time: %q", buf.String())
	}
}

func TestPlainLoggerColors(t *testing.T) {
	var buf bytes.Buffer

	LogFlags{Color: "never"}.NewLogger(&buf).Info("hello", "namespace", "api")
	if strings.Contains(buf.String(), escape) {
		t.Errorf("--color=never must not emit escapes: %q", buf.String())
	}

	buf.Reset()
	LogFlags{Color: "auto"}.NewLogger(&buf).Info("hello", "namespace", "api")
	if strings.Contains(buf.String(), escape) {
		t.Errorf("auto must not color a writer that is not a terminal: %q", buf.String())
	}

	buf.Reset()
	LogFlags{Color: "always"}.NewLogger(&buf).Info("hello", "namespace", "api", "other", "x")
	// namespace is one of the colored keys, other is not: tint colors the key of
	// a tinted attribute and only dims the key of a plain one.
	if !strings.Contains(buf.String(), "95mnamespace=") || strings.Contains(buf.String(), "95mother=") {
		t.Errorf("only the namespace attribute should be colored: %q", buf.String())
	}
}

func TestVerboseEnablesDebug(t *testing.T) {
	var buf bytes.Buffer
	LogFlags{Color: "never"}.NewLogger(&buf).Debug("select 1")
	if buf.Len() != 0 {
		t.Errorf("debug must be hidden by default: %q", buf.String())
	}
	LogFlags{Color: "never", Verbose: true}.NewLogger(&buf).Debug("select 1")
	if !strings.Contains(buf.String(), "select 1") {
		t.Errorf("-v must show debug records: %q", buf.String())
	}
}

func TestSourceIsOptIn(t *testing.T) {
	var buf bytes.Buffer
	LogFlags{Color: "never"}.NewLogger(&buf).Info("hello")
	if strings.Contains(buf.String(), "logging_test.go") {
		t.Errorf("no source by default: %q", buf.String())
	}
	buf.Reset()
	LogFlags{Color: "never", Source: true}.NewLogger(&buf).Info("hello")
	if !strings.Contains(buf.String(), "logging_test.go") {
		t.Errorf("--source must show the file: %q", buf.String())
	}
	buf.Reset()
	LogFlags{JSON: true, Source: true}.NewLogger(&buf).Info("hello")
	if !strings.Contains(buf.String(), `"source"`) {
		t.Errorf("--source must work in JSON too: %q", buf.String())
	}
}

func pgLikeError() error {
	return oops.In("pg").Code("pg_error").
		With("sql", "select\n  nope from t").
		With("position", 10).
		With("detail", "no such column").
		With("sqlstate", "42703").
		Errorf(`ERROR: column "nope" does not exist (SQLSTATE 42703)`)
}

func TestReportErrorPlainShowsMessageDetailAndSQLBlockWithoutStack(t *testing.T) {
	var buf bytes.Buffer
	flags := LogFlags{Color: "never"}
	flags.reportError(flags.NewLogger(&buf), &buf, pgLikeError())

	out := buf.String()
	for _, want := range []string{`column "nope" does not exist`, "detail=", "sqlstate=42703", "    select\n      nope from t"} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "error=") || strings.Contains(out, "sql=") || strings.Contains(out, "position=") {
		t.Errorf("message and sql are shown once, not repeated as attributes:\n%s", out)
	}
	if strings.Contains(out, "goroutine") || strings.Contains(out, ".go:") {
		t.Errorf("no stack trace expected:\n%s", out)
	}
}

func TestReportErrorJSONIsOneRecordWithRawAttributes(t *testing.T) {
	var buf bytes.Buffer
	flags := LogFlags{JSON: true}
	flags.reportError(flags.NewLogger(&buf), &buf, pgLikeError())

	if strings.Count(buf.String(), "\n") != 1 || strings.Contains(buf.String(), escape) {
		t.Fatalf("expected exactly one escape-free line: %q", buf.String())
	}
	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatal(err)
	}
	if rec["level"] != "ERROR" || rec["sql"] != "select\n  nope from t" || rec["position"] != float64(10) ||
		rec["sqlstate"] != "42703" || !strings.Contains(rec["error"].(string), "does not exist") {
		t.Errorf("unexpected record: %v", rec)
	}
}

func TestFormatSQLBlock(t *testing.T) {
	if got := formatSQLBlock("select 1\nfrom x", 0, false); got != "    select 1\n    from x\n" {
		t.Errorf("unexpected plain block: %q", got)
	}
	// position counts characters, not bytes: é is one character
	got := formatSQLBlock("sélect nope", 8, true)
	want := "    \x1b[94msélect \x1b[0m\x1b[91mnope\x1b[0m\n"
	if got != want {
		t.Errorf("expected %q, got %q", want, got)
	}
	// a position past the end must not panic and highlights nothing
	if got := formatSQLBlock("abc", 99, true); !strings.Contains(got, "abc") || strings.Contains(got, "\x1b[91m") {
		t.Errorf("unexpected block for an out of range position: %q", got)
	}
}

package mutations

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/samber/oops"
)

// recorder is a slog.Handler remembering every record it is given, with the
// attributes of the logger that emitted it.
type recorder struct {
	attrs []slog.Attr
	out   *[]map[string]any
}

func newRecorder() *recorder { return &recorder{out: &[]map[string]any{}} }

func (r *recorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *recorder) Handle(_ context.Context, rec slog.Record) error {
	m := map[string]any{"msg": rec.Message}
	for _, a := range r.attrs {
		m[a.Key] = a.Value.Any()
	}
	rec.Attrs(func(a slog.Attr) bool {
		m[a.Key] = a.Value.Any()
		return true
	})
	*r.out = append(*r.out, m)
	return nil
}

func (r *recorder) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &recorder{attrs: append(append([]slog.Attr{}, r.attrs...), attrs...), out: r.out}
}

func (r *recorder) WithGroup(string) slog.Handler { return r }

func TestCaptureHandlerPassesThroughWhenNotCapturing(t *testing.T) {
	rec := newRecorder()
	core := &captureCore{}
	log := slog.New(captureHandler{core: core, inner: rec})

	log.Info("hello", "k", "v")

	if len(*rec.out) != 1 || (*rec.out)[0]["msg"] != "hello" {
		t.Fatalf("expected the record to go straight through, got %v", *rec.out)
	}
	if _, ok := (*rec.out)[0]["phase"]; ok {
		t.Error("a record emitted outside of a capture must not be tagged with a phase")
	}
}

func TestCaptureHandlerReplaysHeldRecordsInOrderWithTheirAttributes(t *testing.T) {
	rec := newRecorder()
	core := &captureCore{}
	log := slog.New(captureHandler{core: core, inner: rec})
	child := log.With("namespace", "api")

	core.beginCapture(Capture{Phase: "test"})
	log.Info("first")
	child.Info("second", "n", 2)
	if len(*rec.out) != 0 {
		t.Fatalf("nothing must be emitted while capturing, got %v", *rec.out)
	}

	core.endCapture(true, nil)
	got := *rec.out
	if len(got) != 2 || got[0]["msg"] != "first" || got[1]["msg"] != "second" {
		t.Fatalf("expected both records replayed in order, got %v", got)
	}
	if got[1]["namespace"] != "api" || got[1]["n"] != int64(2) {
		t.Errorf("a replayed record must keep the attributes of its logger, got %v", got[1])
	}
	for _, m := range got {
		if m["phase"] != "test" {
			t.Errorf("replayed records must carry phase=test, got %v", m)
		}
	}

	log.Info("after")
	if len(*rec.out) != 3 || (*rec.out)[2]["phase"] != nil {
		t.Errorf("logging must be back to normal after the capture, got %v", *rec.out)
	}
}

func TestCaptureHandlerDropsHeldRecordsWithoutReplay(t *testing.T) {
	rec := newRecorder()
	core := &captureCore{}
	log := slog.New(captureHandler{core: core, inner: rec})

	core.beginCapture(Capture{Phase: "test"})
	log.Info("hidden")
	core.endCapture(false, nil)

	if len(*rec.out) != 0 {
		t.Fatalf("records of a successful capture must be dropped, got %v", *rec.out)
	}
	// a later capture must not resurrect them either
	core.beginCapture(Capture{Phase: "test"})
	core.endCapture(true, nil)
	if len(*rec.out) != 0 {
		t.Fatalf("dropped records came back: %v", *rec.out)
	}
}

func TestRedactURIHidesThePassword(t *testing.T) {
	got := redactURI("postgres://user:s3cret@localhost:5432/db?sslmode=disable")
	if strings.Contains(got, "s3cret") {
		t.Errorf("password leaked: %s", got)
	}
	if !strings.Contains(got, "user") || !strings.Contains(got, "localhost:5432/db") {
		t.Errorf("only the password should be hidden, got %s", got)
	}
	if got := redactURI("postgres://localhost/db"); got != "postgres://localhost/db" {
		t.Errorf("a uri without password must be unchanged, got %s", got)
	}
	if got := redactURI("postgres://user:pw@host:notaport/db"); strings.Contains(got, "pw") {
		t.Errorf("an unparsable uri must not be echoed back, got %s", got)
	}
}

func TestErrorAttrsFlattensOopsContextWithoutStack(t *testing.T) {
	inner := oops.In("pg").With("sql", "select 1").With("position", 7).Errorf("boom")
	outer := oops.With("mutation", "api.users").Wrap(inner)

	attrs := ErrorAttrs(outer)
	got := map[string]any{}
	for i := 0; i < len(attrs); i += 2 {
		got[attrs[i].(string)] = attrs[i+1]
	}

	if got["error"] != "boom" {
		t.Errorf("expected error=boom, got %v", got["error"])
	}
	for key, want := range map[string]any{"sql": "select 1", "position": 7, "mutation": "api.users"} {
		if got[key] != want {
			t.Errorf("expected %s=%v from anywhere in the chain, got %v", key, want, got[key])
		}
	}
	if _, ok := got["stacktrace"]; ok || len(got) != 4 {
		t.Errorf("only error and context entries are expected, got %v", got)
	}
	if ErrorAttrs(nil) != nil {
		t.Error("a nil error has no attributes")
	}
}

func TestCaptureOnlyHoldsWhatMatchAccepts(t *testing.T) {
	rec := newRecorder()
	core := &captureCore{}
	log := slog.New(captureHandler{core: core, inner: rec})

	core.beginCapture(Capture{Match: func(r slog.Record) bool { return r.Message == "held" }})
	log.Info("live")
	log.Info("held")
	if len(*rec.out) != 1 || (*rec.out)[0]["msg"] != "live" {
		t.Fatalf("only the rejected record goes through while capturing, got %v", *rec.out)
	}

	core.endCapture(true, nil)
	if len(*rec.out) != 2 || (*rec.out)[1]["msg"] != "held" {
		t.Fatalf("expected the held record to be replayed, got %v", *rec.out)
	}
	if _, ok := (*rec.out)[1]["phase"]; ok {
		t.Error("a capture without a Phase must not tag its records")
	}
}

func TestCaptureNoteIntroducesAReplayOnlyWhenThereIsOne(t *testing.T) {
	rec := newRecorder()
	core := &captureCore{}
	log := slog.New(captureHandler{core: core, inner: rec})
	note := func() { log.Info("note") }

	core.beginCapture(Capture{})
	core.endCapture(true, note)
	if len(*rec.out) != 0 {
		t.Fatalf("nothing was held, so no note either, got %v", *rec.out)
	}

	core.beginCapture(Capture{})
	log.Info("a")
	core.endCapture(true, note)
	if len(*rec.out) != 2 || (*rec.out)[0]["msg"] != "note" || (*rec.out)[1]["msg"] != "a" {
		t.Fatalf("expected the note before the replayed record, got %v", *rec.out)
	}
}

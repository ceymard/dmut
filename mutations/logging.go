package mutations

import (
	"context"
	"log/slog"
	"net/url"
	"sort"
	"sync"

	"github.com/samber/oops"
)

// IconKey is the attribute carrying the icon of a milestone record (see the Icon
// constants). The dmut CLI turns it into a prefix of the message in plain text
// logs and drops it from JSON logs; a handler that knows nothing about it just
// sees an ordinary string attribute.
const IconKey = "icon"

// Icons of the milestones, passed as the IconKey attribute.
const (
	IconTestsStarted = "🧪"
	IconTestsPassed  = "🆗"
	IconNoErrors     = "🎉"
	IconCommitted    = "✅"
	IconRolledBack   = "⛔"
	IconUp           = "🟢" // a runnable going up
	IconDown         = "🔴" // a runnable going down
)

// Capture selects which records a capture holds back and how they are tagged.
type Capture struct {
	// Phase, when not empty, is added to every held record as the "phase" attribute.
	Phase string
	// Match, when not nil, restricts the capture to the records it accepts; the
	// others are emitted live. nil holds every record.
	Match func(slog.Record) bool
}

// captureCore is the state shared by every logger derived from one runner's
// Logger(): whether records are currently being held back, which ones, and the
// held records.
type captureCore struct {
	mu        sync.Mutex
	capturing bool
	capture   Capture
	held      []heldRecord
}

// heldRecord keeps the handler a record was addressed to (with the attributes
// of the logger that emitted it already applied), so a replay prints it exactly
// as it would have been printed live.
type heldRecord struct {
	handler slog.Handler
	record  slog.Record
}

// captureHandler forwards records to inner, unless its core is capturing: then
// records the capture accepts are held until the core is flushed.
type captureHandler struct {
	core  *captureCore
	inner slog.Handler
}

func (h captureHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h captureHandler) Handle(ctx context.Context, r slog.Record) error {
	h.core.mu.Lock()
	if h.core.capturing && (h.core.capture.Match == nil || h.core.capture.Match(r)) {
		r = r.Clone()
		if h.core.capture.Phase != "" {
			r.AddAttrs(slog.String("phase", h.core.capture.Phase))
		}
		h.core.held = append(h.core.held, heldRecord{handler: h.inner, record: r})
		h.core.mu.Unlock()
		return nil
	}
	h.core.mu.Unlock()
	return h.inner.Handle(ctx, r)
}

func (h captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return captureHandler{core: h.core, inner: h.inner.WithAttrs(attrs)}
}

func (h captureHandler) WithGroup(name string) slog.Handler {
	return captureHandler{core: h.core, inner: h.inner.WithGroup(name)}
}

// beginCapture starts holding back the records c accepts instead of emitting them.
func (c *captureCore) beginCapture(capture Capture) {
	c.mu.Lock()
	c.capturing = true
	c.capture = capture
	c.held = nil
	c.mu.Unlock()
}

// endCapture stops holding records back; when replay is true the held records
// are emitted, in order, otherwise they are dropped. note, when not nil, is
// called right before a non-empty replay so it can introduce it.
func (c *captureCore) endCapture(replay bool, note func()) {
	c.mu.Lock()
	held := c.held
	c.held = nil
	c.capturing = false
	c.mu.Unlock()
	if !replay || len(held) == 0 {
		return
	}
	if note != nil {
		note()
	}
	for _, h := range held {
		_ = h.handler.Handle(context.Background(), h.record)
	}
}

// redactURI hides the password of a connection URI, so it can be logged.
// Anything that doesn't parse as a URI with a password is returned unchanged
// unless it can't be parsed at all, in which case it is dropped entirely.
func redactURI(uri string) string {
	u, err := url.Parse(uri)
	if err != nil {
		return "<unparsable uri>"
	}
	return u.Redacted()
}

// ErrorAttrs returns err as a flat run of alternating key/value arguments,
// meant to be spread into a slog call: "error" holds err.Error(), followed by
// every oops .With(key, value) context entry attached anywhere in err's chain
// as a sibling pair (sql, position, detail, hint, sqlstate, mutation, ...).
// The stack trace is deliberately left out.
func ErrorAttrs(err error) []any {
	if err == nil {
		return nil
	}
	attrs := []any{"error", err.Error()}
	if oopsErr, ok := oops.AsOops(err); ok {
		keys := make([]string, 0, len(oopsErr.Context()))
		for k := range oopsErr.Context() {
			keys = append(keys, k)
		}
		sort.Strings(keys) // map order is random; keep the output stable
		for _, k := range keys {
			attrs = append(attrs, k, oopsErr.Context()[k])
		}
	}
	return attrs
}

package mutations

import (
	"context"
	"log/slog"
	"net/url"
	"sort"
	"sync"

	"github.com/samber/oops"
)

// captureCore is the state shared by every logger derived from one runner's
// Logger(): whether records are currently being held back, and the held records.
type captureCore struct {
	mu        sync.Mutex
	capturing bool
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
// records are held (tagged phase=test) until the core is flushed.
type captureHandler struct {
	core  *captureCore
	inner slog.Handler
}

func (h captureHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h captureHandler) Handle(ctx context.Context, r slog.Record) error {
	h.core.mu.Lock()
	if h.core.capturing {
		r = r.Clone()
		r.AddAttrs(slog.String("phase", "test"))
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

// beginCapture starts holding back records instead of emitting them.
func (c *captureCore) beginCapture() {
	c.mu.Lock()
	c.capturing = true
	c.held = nil
	c.mu.Unlock()
}

// endCapture stops holding records back; when replay is true the held records
// are emitted first, in order, otherwise they are dropped.
func (c *captureCore) endCapture(replay bool) {
	c.mu.Lock()
	held := c.held
	c.held = nil
	c.capturing = false
	c.mu.Unlock()
	if !replay {
		return
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

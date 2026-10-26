package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/ceymard/dmut/v3/mutations"
	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"
)

// LogFlags are the global flags shaping how dmut logs. All logs go to stderr;
// stdout is reserved for command results.
type LogFlags struct {
	JSON    bool   `short:"j" name:"json" help:"Log as JSON lines instead of plain text."`
	Verbose bool   `short:"v" name:"verbose" help:"Log at debug level, which includes every SQL statement."`
	Source  bool   `name:"source" help:"Add the source file and line to each log record."`
	Time    bool   `name:"time" help:"Show timestamps in plain text logs (JSON logs always have one)."`
	Color   string `name:"color" enum:"auto,always,never" default:"auto" help:"Colorize plain text logs: auto, always or never."`
}

// useColor decides whether plain logs written to w are colored: "always" and
// "never" are explicit, "auto" requires a terminal and an unset NO_COLOR.
func (f LogFlags) useColor(w io.Writer) bool {
	switch f.Color {
	case "always":
		return true
	case "never":
		return false
	}
	file, ok := w.(*os.File)
	return ok && os.Getenv("NO_COLOR") == "" && isatty.IsTerminal(file.Fd())
}

// Terminal 256-color codes for the attributes worth spotting at a glance.
var attrColors = map[string]uint8{
	"namespace": 13, // bright magenta
	"revision":  10, // bright green
	"mutation":  14, // bright cyan
	"sql":       12, // bright blue
	"error":     9,  // bright red
}

// Colors of attributes whose meaning depends on their value.
var valueColors = map[string]map[string]uint8{
	"dir": {"up": 10, "down": 9}, // bright green, bright red
}

// iconHandler takes the mutations.IconKey attribute out of each record. In plain
// mode it is shown in front of the message, in JSON mode it is dropped so that
// msg stays plain text.
type iconHandler struct {
	inner slog.Handler
	show  bool
}

func (h iconHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h iconHandler) Handle(ctx context.Context, r slog.Record) error {
	var icon string
	kept := make([]slog.Attr, 0, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == mutations.IconKey {
			icon = a.Value.String()
		} else {
			kept = append(kept, a)
		}
		return true
	})
	if icon == "" {
		return h.inner.Handle(ctx, r)
	}

	msg := r.Message
	if h.show {
		msg = icon + " " + msg
	}
	// r.PC is carried over so --source still points at the logging call.
	out := slog.NewRecord(r.Time, r.Level, msg, r.PC)
	out.AddAttrs(kept...)
	return h.inner.Handle(ctx, out)
}

func (h iconHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return iconHandler{inner: h.inner.WithAttrs(attrs), show: h.show}
}

func (h iconHandler) WithGroup(name string) slog.Handler {
	return iconHandler{inner: h.inner.WithGroup(name), show: h.show}
}

// NewLogger builds the logger described by the flags, writing to w.
func (f LogFlags) NewLogger(w io.Writer) *slog.Logger {
	level := slog.LevelInfo
	if f.Verbose {
		level = slog.LevelDebug
	}
	if f.JSON {
		return slog.New(iconHandler{inner: slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level, AddSource: f.Source})})
	}
	return slog.New(iconHandler{show: true, inner: tint.NewTextHandler(w, &tint.Options{
		Level:      level,
		AddSource:  f.Source,
		NoColor:    !f.useColor(w),
		TimeFormat: "15:04:05",
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 && a.Key == slog.TimeKey && !f.Time {
				return slog.Attr{}
			}
			if len(groups) == 0 {
				if c, ok := attrColors[a.Key]; ok {
					return tint.Attr(c, a)
				}
				if c, ok := valueColors[a.Key][a.Value.String()]; ok {
					return tint.Attr(c, a)
				}
			}
			return a
		},
	})})
}

// reportError logs err as the final error of the run. In JSON mode it is one
// record carrying every error attribute. In plain mode the message is the log
// line, the other attributes follow it, and the failing SQL is printed below as
// an indented block with the part from the error position highlighted.
func (f LogFlags) reportError(logger *slog.Logger, w io.Writer, err error) {
	attrs := mutations.ErrorAttrs(err)
	if f.JSON {
		logger.Error(err.Error(), attrs...)
		return
	}

	var sql string
	var position int
	rest := make([]any, 0, len(attrs))
	for i := 0; i < len(attrs); i += 2 {
		key, value := attrs[i].(string), attrs[i+1]
		switch key {
		case "error": // already the log message
		case "sql":
			sql, _ = value.(string)
		case "position":
			position, _ = value.(int)
		default:
			rest = append(rest, key, value)
		}
	}
	logger.Error(err.Error(), rest...)
	if sql != "" {
		fmt.Fprint(w, formatSQLBlock(sql, position, f.useColor(w)))
	}
}

// formatSQLBlock indents sql by four spaces. position is the 1-based character
// offset reported by Postgres (0 when unknown); with color on, the text before
// it is blue and the text from it onwards is red.
func formatSQLBlock(sql string, position int, color bool) string {
	const blue, red, reset = "\x1b[94m", "\x1b[91m", "\x1b[0m"
	indent := func(s string) string { return strings.ReplaceAll(s, "\n", "\n    ") }

	runes := []rune(sql)
	if position < 1 || position > len(runes) {
		position = len(runes) + 1
	}
	before, after := string(runes[:position-1]), string(runes[position-1:])

	if !color {
		return "    " + indent(before+after) + "\n"
	}
	out := "    " + blue + indent(before) + reset
	if after != "" {
		out += red + indent(after) + reset
	}
	return out + "\n"
}

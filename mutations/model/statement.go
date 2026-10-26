package model

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/ceymard/dmut/v2/mutations"
	"github.com/goccy/go-yaml"
	"github.com/samber/oops"
)

// where locates an error or warning in the files.
type where struct {
	file     string
	mutation string
	position int
	warnings *[]Warning
}

func (w where) prefix() string {
	p := w.file
	if w.mutation != "" {
		p += ": mutation " + w.mutation
	}
	if w.position > 0 {
		p += ": statement " + strconv.Itoa(w.position)
	}
	return p
}

func (w where) errf(format string, args ...any) error {
	return oops.In("model").With("file", w.file).With("mutation", w.mutation).With("position", w.position).
		Errorf("%s: %s", w.prefix(), fmt.Sprintf(format, args...))
}

func (w where) wrap(err error) error {
	return oops.In("model").With("file", w.file).With("mutation", w.mutation).With("position", w.position).
		Wrapf(err, "%s", w.prefix())
}

func (w where) warn(format string, args ...any) {
	*w.warnings = append(*w.warnings, Warning{File: w.file, Mutation: w.mutation, Position: w.position, Message: fmt.Sprintf(format, args...)})
}

// parseStatement reads one entry of a sql list: a string, or a mapping with one of
// auto, up (with down), seed or ensure, plus optional settings and, for seeds, data.
func parseStatement(w where, raw any) (Statement, error) {
	var st Statement
	switch v := raw.(type) {
	case string:
		auto, err := autoStatement(w, v)
		if err != nil {
			return st, err
		}
		st = auto
	case yaml.MapSlice:
		parsed, err := parseStatementMap(w, v)
		if err != nil {
			return st, err
		}
		st = parsed
	default:
		return st, w.errf("expected a string or a mapping, got %T", raw)
	}
	if err := st.finish(); err != nil {
		return st, w.wrap(err)
	}
	return st, nil
}

func parseStatementMap(w where, m yaml.MapSlice) (Statement, error) {
	var st Statement
	var mainKey, text, down string
	var hasDown, hasData bool
	var settings, data any
	for _, item := range m {
		key, _ := item.Key.(string)
		switch key {
		case "auto", "up", "seed", "ensure":
			if mainKey != "" {
				return st, w.errf("a statement has one of auto, up, seed or ensure, got %s and %s", mainKey, key)
			}
			s, ok := item.Value.(string)
			if !ok {
				return st, w.errf("%s must be a string, got %T", key, item.Value)
			}
			mainKey, text = key, s
		case "down":
			s, ok := item.Value.(string)
			if !ok {
				return st, w.errf("down must be a string, got %T", item.Value)
			}
			down, hasDown = s, true
		case "settings":
			settings = item.Value
		case "data":
			data, hasData = item.Value, true
		default:
			return st, w.errf("unknown statement key %v", item.Key)
		}
	}
	if mainKey == "" {
		return st, w.errf("a statement needs one of auto, up, seed or ensure")
	}
	if strings.TrimSpace(text) == "" {
		return st, w.errf("%s is empty", mainKey)
	}
	if hasDown && mainKey != "up" {
		return st, w.errf("down is only allowed next to up")
	}
	if hasData && mainKey != "seed" {
		return st, w.errf("data is only allowed next to seed")
	}

	switch mainKey {
	case "auto":
		auto, err := autoStatement(w, text)
		if err != nil {
			return st, err
		}
		st = auto
	case "up":
		if !hasDown || strings.TrimSpace(down) == "" {
			return st, w.errf("up needs a non-empty down ; use seed for a statement with no way back")
		}
		st = Statement{Kind: KindReversible, Up: text, Down: down, explicitDown: true}
	case "seed":
		st = Statement{Kind: KindSeed, Up: text}
		if hasData {
			rows, err := parseData(w, data, text)
			if err != nil {
				return st, err
			}
			st.Data = rows
		}
	case "ensure":
		toks, err := sqlStatements(text)
		if err != nil {
			return st, w.wrap(err)
		}
		if len(toks) != 1 || (words(toks[0], 1)[0] != "select" && words(toks[0], 1)[0] != "with") {
			return st, w.errf("ensure must be a single SELECT or WITH statement")
		}
		st = Statement{Kind: KindCheck, Up: text}
	}

	list, err := parseSettings(w, settings)
	if err != nil {
		return st, err
	}
	st.Settings = list
	return st, nil
}

// autoStatement builds a statement whose down dmut infers from its up. A statement with
// nothing to undo (COMMENT ON, ALTER TYPE ... ADD VALUE IF NOT EXISTS) becomes a seed.
func autoStatement(w where, sql string) (Statement, error) {
	if strings.TrimSpace(sql) == "" {
		return Statement{}, w.errf("statement is empty")
	}
	if stmts, err := sqlStatements(sql); err != nil {
		return Statement{}, w.wrap(err)
	} else if len(stmts) > 1 {
		return Statement{}, w.errf("an automatic statement must be a single SQL statement ; use up/down for several")
	}
	if add, ifNotExists := alterTypeAddValue(sql); add {
		if !ifNotExists {
			return Statement{}, w.errf("ALTER TYPE ... ADD VALUE has no automatic down ; write IF NOT EXISTS, or use up/down or seed")
		}
		return Statement{Kind: KindSeed, Up: sql}, nil
	}
	down, noDown, err := mutations.AutoDowner.ParseAndGetDefault(sql)
	if err != nil {
		return Statement{}, w.errf("can't infer a down ; write it with up/down, or use seed: %v", err)
	}
	if noDown {
		return Statement{Kind: KindSeed, Up: sql}, nil
	}
	if down == "" {
		return Statement{}, w.errf("empty down inferred ; write it with up/down")
	}
	return Statement{Kind: KindReversible, Up: sql, Down: down}, nil
}

// alterTypeAddValue reports whether sql is ALTER TYPE ... ADD VALUE, and whether it says IF NOT EXISTS.
func alterTypeAddValue(sql string) (add, ifNotExists bool) {
	stmts, err := sqlStatements(sql)
	if err != nil || len(stmts) == 0 {
		return false, false
	}
	w := words(stmts[0], 8)
	if len(w) < 5 || w[0] != "alter" || w[1] != "type" || w[3] != "add" || w[4] != "value" {
		return false, false
	}
	return true, len(w) >= 8 && w[5] == "if" && w[6] == "not" && w[7] == "exists"
}

func parseSettings(w where, raw any) ([]Setting, error) {
	if raw == nil {
		return nil, nil
	}
	items, ok := raw.([]any)
	if !ok {
		return nil, w.errf("settings must be a list, got %T", raw)
	}
	seen := map[string]bool{}
	var out []Setting
	for _, item := range items {
		s := Setting{Tracked: true}
		switch v := item.(type) {
		case string:
			s.Name = v
		case yaml.MapSlice:
			for _, kv := range v {
				key, _ := kv.Key.(string)
				switch key {
				case "name":
					name, ok := kv.Value.(string)
					if !ok {
						return nil, w.errf("setting name must be a string, got %T", kv.Value)
					}
					s.Name = name
				case "tracked", "saved":
					b, ok := kv.Value.(bool)
					if !ok {
						return nil, w.errf("setting %s must be a boolean, got %T", key, kv.Value)
					}
					if key == "tracked" {
						s.Tracked = b
					} else {
						s.Saved = b
					}
				default:
					return nil, w.errf("unknown setting key %v", kv.Key)
				}
			}
		default:
			return nil, w.errf("a setting is a name or a mapping, got %T", item)
		}
		if err := validSettingName(s.Name); err != nil {
			return nil, w.errf("%v", err)
		}
		lower := strings.ToLower(s.Name)
		if seen[lower] {
			return nil, w.errf("setting %s is listed twice", s.Name)
		}
		seen[lower] = true
		if s.Saved && looksSecret(s.Name) {
			w.warn("setting %s is saved in clear in the database and its name looks like a secret", s.Name)
		}
		out = append(out, s)
	}
	return out, nil
}

// validSettingName wants `dmut.` followed by at least one more dot-separated part.
func validSettingName(name string) error {
	parts := strings.Split(name, ".")
	if len(parts) < 3 || !strings.EqualFold(parts[0], "dmut") {
		return fmt.Errorf("setting name %q must start with dmut. and have at least one more dot", name)
	}
	for _, p := range parts {
		if p == "" {
			return fmt.Errorf("setting name %q has an empty part", name)
		}
	}
	return nil
}

var secretWords = []string{"secret", "password", "passwd", "pwd", "token", "credential", "private", "auth", "apikey", "key"}

func looksSecret(name string) bool {
	lower := strings.ToLower(name)
	for _, word := range secretWords {
		if strings.Contains(lower, word) {
			return true
		}
	}
	return false
}

// parseData reads the rows of a seed. Every row must be as long as the highest $n of the statement.
func parseData(w where, raw any, sql string) ([][]*string, error) {
	list, ok := raw.([]any)
	if !ok {
		return nil, w.errf("data must be a list of rows, got %T", raw)
	}
	stmts, err := sqlStatements(sql)
	if err != nil {
		return nil, w.wrap(err)
	}
	if len(stmts) != 1 {
		return nil, w.errf("a statement with data must be a single SQL statement")
	}
	params := 0
	for _, tok := range stmts[0] {
		if n, ok := parameterIndex(tok.Value); ok && n > params {
			params = n
		}
	}
	rows := make([][]*string, 0, len(list))
	for i, rawRow := range list {
		row, ok := rawRow.([]any)
		if !ok {
			return nil, w.errf("data row %d must be a list, got %T", i+1, rawRow)
		}
		if len(row) != params {
			return nil, w.errf("data row %d has %d values but the statement uses $1 to $%d", i+1, len(row), params)
		}
		out := make([]*string, len(row))
		for j, v := range row {
			text, err := dataText(v)
			if err != nil {
				return nil, w.errf("data row %d value %d: %v", i+1, j+1, err)
			}
			out[j] = text
		}
		rows = append(rows, out)
	}
	return rows, nil
}

func parameterIndex(tok string) (int, bool) {
	if len(tok) < 2 || tok[0] != '$' {
		return 0, false
	}
	n, err := strconv.Atoi(tok[1:])
	return n, err == nil && n > 0
}

// dataText renders a yaml value as the text Postgres reads: null is NULL, a map or a list is JSON.
func dataText(v any) (*string, error) {
	var s string
	switch x := v.(type) {
	case nil:
		return nil, nil
	case string:
		s = x
	case bool:
		s = strconv.FormatBool(x)
	case int:
		s = strconv.Itoa(x)
	case int64:
		s = strconv.FormatInt(x, 10)
	case uint64:
		s = strconv.FormatUint(x, 10)
	case float64:
		s = strconv.FormatFloat(x, 'f', -1, 64)
	default:
		j, err := json.Marshal(jsonable(v))
		if err != nil {
			return nil, err
		}
		s = string(j)
	}
	return &s, nil
}

// jsonable turns ordered yaml maps into values encoding/json can write.
func jsonable(v any) any {
	switch x := v.(type) {
	case yaml.MapSlice:
		m := make(map[string]any, len(x))
		for _, kv := range x {
			m[fmt.Sprint(kv.Key)] = jsonable(kv.Value)
		}
		return m
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = jsonable(e)
		}
		return out
	}
	return v
}

// finish computes the text and data hashes.
func (s *Statement) finish() error {
	th, err := TextHash(s.Kind, s.Up, s.Down)
	if err != nil {
		return err
	}
	s.TextHash = th
	s.DataHash = dataHash(s.Data)
	return nil
}

// TextHash identifies a statement by its kind and its normalized up and down text:
// whitespace and comments do not change it.
func TextHash(kind Kind, up, down string) (string, error) {
	dg := mutations.NewDigestBuffer()
	dg.WriteString(string(kind))
	dg.WriteByte(0)
	if err := dg.AddStatement(up); err != nil {
		return "", err
	}
	dg.WriteByte(0)
	if err := dg.AddStatement(down); err != nil {
		return "", err
	}
	sum := sha256.Sum256(dg.Bytes())
	return base64.StdEncoding.EncodeToString(sum[:]), nil
}

// dataHash is empty for a statement with no data, so it does not show in its digest.
func dataHash(rows [][]*string) string {
	if rows == nil {
		return ""
	}
	h := sha256.New()
	var n [8]byte
	for _, row := range rows {
		binary.BigEndian.PutUint64(n[:], uint64(len(row)))
		h.Write(n[:])
		for _, v := range row {
			if v == nil {
				h.Write([]byte{0})
				continue
			}
			binary.BigEndian.PutUint64(n[:], uint64(len(*v)))
			h.Write([]byte{1})
			h.Write(n[:])
			h.Write([]byte(*v))
		}
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

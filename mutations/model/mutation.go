package model

import (
	"fmt"

	"github.com/goccy/go-yaml"
)

// parseMutation reads the keys of one mutation: sql, structural, deps and reapply_on.
func parseMutation(w where, name, namespace string, raw any) (*Mutation, error) {
	if !validName(name) {
		return nil, w.errf("%q is not a valid mutation name ; use letters, digits, _, - and dots", name)
	}
	w.mutation = name
	m, ok := raw.(yaml.MapSlice)
	if !ok && raw != nil {
		return nil, w.errf("expected a mapping describing a mutation, got %T", raw)
	}

	mut := &Mutation{Name: name, Namespace: namespace, File: w.file}
	var sqlRaw any
	for _, item := range m {
		key, _ := item.Key.(string)
		switch key {
		case "sql":
			sqlRaw = item.Value
		case "structural":
			b, ok := item.Value.(bool)
			if !ok {
				return nil, w.errf("structural must be a boolean, got %T", item.Value)
			}
			mut.Structural = b
		case "deps":
			deps, err := parseDeps(w, item.Value)
			if err != nil {
				return nil, err
			}
			mut.Deps = deps
		case "reapply_on":
			list, err := parseTriggers(w, item.Value)
			if err != nil {
				return nil, err
			}
			mut.ReapplyOn = list
		default:
			return nil, w.errf("unknown key %v", item.Key)
		}
	}

	if sqlRaw == nil {
		return mut, nil
	}
	list, ok := sqlRaw.([]any)
	if !ok {
		return nil, w.errf("sql must be a list of statements, got %T", sqlRaw)
	}
	// Doc statements go after everything the author wrote, so that they can change without
	// disturbing the statements before them.
	var docs []Statement
	for i, rawStmt := range list {
		sw := w
		sw.position = i + 1
		st, stDocs, err := parseStatement(sw, rawStmt)
		if err != nil {
			return nil, err
		}
		if err := checkStatement(sw, st, mut.Structural); err != nil {
			return nil, err
		}
		mut.Statements = append(mut.Statements, st)
		docs = append(docs, stDocs...)
	}
	for i, st := range docs {
		sw := w
		sw.position = len(list) + i + 1
		if err := checkStatement(sw, st, mut.Structural); err != nil {
			return nil, err
		}
	}
	mut.Statements = append(mut.Statements, docs...)
	return mut, nil
}

// checkStatement runs the load-time SQL checks on every text the statement can run.
func checkStatement(w where, st Statement, structural bool) error {
	if err := checkSQL(st.Up, structural); err != nil {
		return w.wrap(err)
	}
	if st.explicitDown {
		if err := checkSQL(st.Down, structural); err != nil {
			return w.wrap(err)
		}
	}
	return nil
}

func parseDeps(w where, raw any) ([]DepEntry, error) {
	if raw == nil {
		return nil, nil
	}
	list, ok := raw.([]any)
	if !ok {
		return nil, w.errf("deps must be a list, got %T", raw)
	}
	out := make([]DepEntry, 0, len(list))
	for _, item := range list {
		var entry DepEntry
		switch v := item.(type) {
		case string:
			entry.Pattern = v
		case yaml.MapSlice:
			for _, kv := range v {
				key, _ := kv.Key.(string)
				switch key {
				case "name":
					s, ok := kv.Value.(string)
					if !ok {
						return nil, w.errf("dependency name must be a string, got %T", kv.Value)
					}
					entry.Pattern = s
				case "reapply_on":
					triggers, err := parseTriggers(w, kv.Value)
					if err != nil {
						return nil, err
					}
					entry.ReapplyOn = triggers
				default:
					return nil, w.errf("unknown dependency key %v", kv.Key)
				}
			}
		default:
			return nil, w.errf("a dependency is a name, a pattern or a mapping, got %T", item)
		}
		if !validPattern(entry.Pattern) {
			return nil, w.errf("%q is not a valid dependency ; use names and the segments * and **", entry.Pattern)
		}
		out = append(out, entry)
	}
	return out, nil
}

// parseTriggers returns a non-nil slice even for an empty list, so "no trigger" differs from "not set".
func parseTriggers(w where, raw any) ([]Trigger, error) {
	list, ok := raw.([]any)
	if !ok {
		return nil, w.errf("reapply_on must be a list, got %T", raw)
	}
	out := make([]Trigger, 0, len(list))
	for _, item := range list {
		s, ok := item.(string)
		if !ok || !validTrigger(Trigger(s)) {
			return nil, w.errf("unknown reapply trigger %v ; expected one of %v", item, allTriggers)
		}
		out = append(out, Trigger(s))
	}
	return out, nil
}

func validTrigger(t Trigger) bool {
	for _, known := range allTriggers {
		if t == known {
			return true
		}
	}
	return false
}

// EffectiveReapplyOn returns the triggers of a dependency entry: its own, else the
// mutation's, else the default of its kind.
func (m *Mutation) EffectiveReapplyOn(entry DepEntry) []Trigger {
	if entry.ReapplyOn != nil {
		return entry.ReapplyOn
	}
	return m.DefaultReapplyOn()
}

// DefaultReapplyOn returns the mutation's own reapply_on, else [dep_reapplied] for a
// default mutation and no trigger for a structural one.
func (m *Mutation) DefaultReapplyOn() []Trigger {
	switch {
	case m.ReapplyOn != nil:
		return m.ReapplyOn
	case m.Structural:
		return []Trigger{}
	}
	return []Trigger{DepReapplied}
}

func (m *Mutation) String() string { return fmt.Sprintf("%s/%s", m.Namespace, m.Name) }

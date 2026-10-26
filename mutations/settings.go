package mutations

import (
	"context"
	"os"
	"sort"
	"strings"

	"github.com/ceymard/dmut/v3/mutations/model"
)

// settingValues holds the resolved value of each setting name used in a project, as written in
// the files. A nil value is unset.
type settingValues map[string]*string

// resolveSettings gives a value to every setting name that a statement of the project lists:
// dmut.env.X reads the environment, dmut.values.N reads opts.Values, and any other name asks
// opts.Settings with the name split on dots.
func resolveSettings(p *model.Project, opts Options) settingValues {
	values := settingValues{}
	visit := func(m *model.Mutation) {
		for _, st := range m.Statements {
			for _, s := range st.Settings {
				if _, done := values[s.Name]; !done {
					values[s.Name] = lookupSetting(s.Name, opts)
				}
			}
		}
	}
	for _, ns := range p.Namespaces {
		for _, rev := range ns.Revisions {
			for _, m := range rev.Mutations {
				visit(m)
			}
			for _, m := range rev.Override {
				visit(m)
			}
		}
	}
	return values
}

func lookupSetting(name string, opts Options) *string {
	parts := strings.Split(name, ".")
	if len(parts) > 2 {
		rest := strings.Join(parts[2:], ".")
		switch strings.ToLower(parts[1]) {
		case "env":
			if v, ok := os.LookupEnv(rest); ok {
				return &v
			}
			return nil
		case "values":
			if v, ok := opts.Values[rest]; ok {
				return &v
			}
			return nil
		}
	}
	if opts.Settings != nil {
		return opts.Settings(parts)
	}
	return nil
}

// digests returns the tracked settings of statement i of m, hashed for its digest.
func (v settingValues) digests(namespace string) model.SettingsFunc {
	return func(m *model.Mutation, i int) []model.SettingDigest {
		var out []model.SettingDigest
		for _, s := range m.Statements[i].Settings {
			if s.Tracked {
				out = append(out, model.SettingDigest{Name: s.Name, Hash: model.SettingHash(namespace, m.Name, i+1, s.Name, v[s.Name])})
			}
		}
		return out
	}
}

// saved returns the values of the saved settings of statement i of m, or nil when none is saved.
func (v settingValues) saved(m *model.Mutation, i int) map[string]*string {
	var out map[string]*string
	for _, s := range m.Statements[i].Settings {
		if s.Saved {
			if out == nil {
				out = map[string]*string{}
			}
			out[s.Name] = v[s.Name]
		}
	}
	return out
}

// setSettings sets names to values for the rest of the transaction; a nil value is set empty.
func (r *Runner) setSettings(ctx context.Context, loc location, names []string, values []*string) error {
	if len(names) == 0 {
		return nil
	}
	texts := make([]string, len(values))
	for i, v := range values {
		if v != nil {
			texts[i] = *v
		}
	}
	return r.execAt(ctx, loc, `select set_config(n, v, true) from unnest($1::text[], $2::text[]) as t(n, v)`, names, texts)
}

// clearSettings sets names to the empty string, which is how Postgres makes a name unset again.
func (r *Runner) clearSettings(ctx context.Context, loc location, names []string) error {
	return r.setSettings(ctx, loc, names, make([]*string, len(names)))
}

// usedSettingNames lists, sorted, every setting name of the project and of the statements recorded
// in state, so that all of them can be set empty at the start of a revision.
func usedSettingNames(p *model.Project, state model.State) []string {
	seen := map[string]bool{}
	add := func(name string) { seen[name] = true }
	for _, ns := range p.Namespaces {
		for _, rev := range ns.Revisions {
			for _, m := range rev.Mutations {
				for _, st := range m.Statements {
					for _, s := range st.Settings {
						add(s.Name)
					}
				}
			}
		}
	}
	for _, rec := range state {
		for _, st := range rec.Statements {
			_, settings := model.ParseDigest(st.Digest)
			for _, s := range settings {
				add(s.Name)
			}
			for name := range st.Saved {
				add(name)
			}
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

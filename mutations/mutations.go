package mutations

import (
	"encoding/json"
	"iter"

	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
	au "github.com/logrusorgru/aurora"
	"github.com/samber/oops"
	"github.com/ugurcsen/gods-generic/sets/hashset"
)

type IterationDirection struct {
	Down bool
	Meta bool
}

func (dir IterationDirection) UpOrDown() string {
	up_or_down := au.BrightGreen("↑").String()
	if dir.Down {
		up_or_down = au.BrightRed("↓").String()
	}
	return up_or_down
}

func (dir IterationDirection) MetaOrSql() string {
	meta_or_sql := au.BrightGreen("sql").String()
	if dir.Meta {
		meta_or_sql = au.BrightCyan("meta").String()
	}
	return meta_or_sql
}

var (
	ITER_SQL_DOWN  = IterationDirection{Down: true, Meta: false}
	ITER_SQL_UP    = IterationDirection{Down: false, Meta: false}
	ITER_META_DOWN = IterationDirection{Down: true, Meta: true}
	ITER_META_UP   = IterationDirection{Down: false, Meta: true}
	ITER_META      = IterationDirection{Down: false, Meta: true}
	ITER_SQL       = IterationDirection{Down: false, Meta: false}
)

type Mutation struct {
	set *MutationSet `json:"-"`

	Name      string `json:"name"`
	File      string `json:"file"`
	Namespace string `json:"namespace"`

	Needs     []string            `json:"needs,omitempty"`
	Sql       []MutationStatement `json:"sql,omitempty"`
	MetaNeeds []string            `json:"meta_needs,omitempty"`
	Meta      []MutationStatement `json:"meta,omitempty"`

	//
	NewNeeds []string            `json:"-"`
	NewSql   []MutationStatement `json:"-"`

	// Will only be used when loading from yaml, not from database
	SqlParents   *hashset.Set[*Mutation] `json:"-"`
	SqlChildren  *hashset.Set[*Mutation] `json:"-"`
	MetaParents  *hashset.Set[*Mutation] `json:"-"`
	MetaChildren *hashset.Set[*Mutation] `json:"-"`

	// Only used during yaml parsing
	ChildrenMutations MutationMap `json:"-"`
}

type SaveableMutation struct {
	Name      string `json:"name"`
	File      string `json:"file"`
	Namespace string `json:"namespace"`

	Needs     []string            `json:"needs,omitempty"`
	Sql       []MutationStatement `json:"sql,omitempty"`
	MetaNeeds []string            `json:"meta_needs,omitempty"`
	Meta      []MutationStatement `json:"meta,omitempty"`
}

func (mut *Mutation) ShouldBeSaved() bool {
	return mut.SqlChildren.Size() > 0 ||
		mut.MetaChildren.Size() > 0 ||
		(mut.NewSql == nil && len(mut.Sql) > 0 || mut.NewSql != nil && len(mut.NewSql) > 0) ||
		len(mut.Meta) > 0
}

func (mut *Mutation) MarshalJSON() ([]byte, error) {

	saveable_mut := &SaveableMutation{
		Name:      mut.Name,
		File:      mut.File,
		Namespace: mut.Namespace,
		Needs:     mut.Needs,
		Sql:       mut.Sql,
		MetaNeeds: mut.MetaNeeds,
		Meta:      mut.Meta,
	}

	if mut.NewNeeds != nil {
		saveable_mut.Needs = mut.NewNeeds
	}
	if mut.NewSql != nil {
		saveable_mut.Sql = mut.NewSql
	}

	return json.Marshal(saveable_mut)
}

func parseStringList(value ast.Node) (list []string, err error) {
	var value_list []string
	if err := yaml.NodeToValue(value, &value_list); err != nil {
		return nil, oops.In("mutations").Wrapf(err, "error decoding value %T", value)
	}
	return value_list, nil
}

func parseMutation(name string, ms *MutationSet, value ast.Node) (mut *Mutation, err error) {
	mutation_def, ok := value.(*ast.MappingNode)
	if !ok {
		return nil, oops.In("mutations").Errorf("in key %s, expected a map describing a mutation, got %T", name, value)
	}

	mut = &Mutation{set: ms, Name: name}
	ms.AddMutation(mut)

	oo := oops.In("mutations").With("mutation", mut.Name).With("file", ms.File).With("namespace", ms.Namespace)

	for _, mapping := range mutation_def.Values {
		key_node := mapping.Key
		var key string
		if err := yaml.NodeToValue(key_node, &key); err != nil {
			return nil, oops.In("mutations").Wrapf(err, "error decoding key %T", key_node)
		}
		value := mapping.Value

		switch key {
		case "needs":
			if list, err := parseStringList(value); err != nil {
				return nil, err
			} else {
				mut.Needs = list
			}
		case "sql":
			if list, err := parseStatements(value); err != nil {
				return nil, err
			} else {
				mut.Sql = list
			}
		case "meta":
			if list, err := parseStatements(value); err != nil {
				return nil, err
			} else {
				mut.Meta = list
			}
		case "meta_needs":
			if list, err := parseStringList(value); err != nil {
				return nil, err
			} else {
				mut.MetaNeeds = list
			}
		case "new_needs":
			if list, err := parseStringList(value); err != nil {
				return nil, err
			} else {
				mut.NewNeeds = list
			}
		case "new_sql":
			if list, err := parseStatements(value); err != nil {
				return nil, err
			} else {
				mut.NewSql = list
			}
		case "children":
			children_def, ok := value.(*ast.MappingNode)
			if !ok {
				return nil, oo.Errorf("'children' must be a map of mutations, got %T", value)
			}

			mut.ChildrenMutations = make(MutationMap)
			for _, mapping := range children_def.Values {
				child_name_node := mapping.Key
				var child_name string
				if err := yaml.NodeToValue(child_name_node, &child_name); err != nil {
					return nil, oo.Wrapf(err, "error decoding child name %T", child_name_node)
				}
				child_value := mapping.Value
				child, err := parseMutation(mut.Name+"."+child_name, mut.set, child_value)
				if err != nil {
					return nil, err
				}
				mut.ChildrenMutations[child_name] = child
			}
		default:
			return nil, oo.Errorf("unknown key '%s'", key)
		}
	}
	return mut, nil
}

func (mut *Mutation) DisplayName() string {
	return au.BrightMagenta(mut.set.Namespace).String() + " " + mut.Name
}

func (mut *Mutation) SqlHash() string {
	digest := NewDigestBuffer()
	digest.WriteString(mut.Name)
	digest.AddStatements(mut.Sql...)
	return digest.Digest()
}

func (mut *Mutation) MetaHash() string {
	digest := NewDigestBuffer()
	digest.WriteString(mut.Name)
	digest.AddStatements(mut.Meta...)
	return digest.Digest()
}

func (mut *Mutation) Hash(dir IterationDirection) string {
	if dir.Meta {
		return mut.MetaHash()
	} else {
		return mut.SqlHash()
	}
}

func (mut *Mutation) IterateDependencies(dir IterationDirection) iter.Seq[*Mutation] {
	return func(yield func(*Mutation) bool) {
		var iterate func(*Mutation) bool
		var seen = hashset.New[*Mutation]()
		iterate = func(mut *Mutation) bool {
			if seen.Contains(mut) {
				return true
			}
			seen.Add(mut)

			var deps *hashset.Set[*Mutation]
			if dir.Down {
				if dir.Meta {
					deps = mut.MetaChildren
				} else {
					deps = mut.SqlChildren
				}
			} else {
				if dir.Meta {
					deps = mut.MetaParents
				} else {
					deps = mut.SqlParents
				}
			}

			for _, dep := range deps.Values() {
				if !iterate(dep) {
					return false
				}
			}

			if !yield(mut) {
				return false
			}

			return true
		}
		iterate(mut)
	}
}

func (mut *Mutation) Runnable(dir IterationDirection) *Runnable {
	return &Runnable{
		Mutation:  mut,
		Direction: dir,
	}
}

func (mut *Mutation) RunRecursively(runner Executor, dir IterationDirection) error {
	for dep := range mut.IterateDependencies(dir) {
		if err := runner.Run(dep.Runnable(dir)); err != nil {
			return err
		}
	}
	return nil
}

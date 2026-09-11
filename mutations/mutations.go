package mutations

import (
	"iter"
	"strings"

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
	meta_or_sql := au.BrightYellow("core").String()
	if dir.Meta {
		meta_or_sql = au.BrightBlue("glue").String()
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
	Core      []MutationStatement `json:"core,omitempty"`
	GlueNeeds []string            `json:"glue_needs,omitempty"`
	Glue      []MutationStatement `json:"glue,omitempty"`

	// Will only be used when loading from yaml, not from database
	SqlParents   *hashset.Set[*Mutation] `json:"-"`
	SqlChildren  *hashset.Set[*Mutation] `json:"-"`
	MetaParents  *hashset.Set[*Mutation] `json:"-"`
	MetaChildren *hashset.Set[*Mutation] `json:"-"`

	// Only used during yaml parsing
	ChildrenMutations MutationMap `json:"-"`
}

func (mut *Mutation) NameComponents() []string {
	return strings.Split(mut.Name, ".")
}

func (mut *Mutation) ShouldBeSaved() bool {
	return len(mut.Core) > 0 || len(mut.Needs) > 0 || len(mut.Glue) > 0 || len(mut.GlueNeeds) > 0
}

func parseStringList(value ast.Node) (list []string, err error) {
	var value_list []string = make([]string, 0)
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
		case "core":
			if list, err := parseStatements(value); err != nil {
				return nil, err
			} else {
				mut.Core = list
			}
		case "glue":
			if list, err := parseStatements(value); err != nil {
				return nil, err
			} else {
				mut.Glue = list
			}
		case "glue_needs":
			if list, err := parseStringList(value); err != nil {
				return nil, err
			} else {
				mut.GlueNeeds = list
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
	if err := ms.AddMutation(mut); err != nil {
		return nil, err
	}
	return mut, nil
}

func (mut *Mutation) DisplayName() string {
	return au.BrightMagenta(mut.set.Namespace).String() + " " + mut.Name
}

func (mut *Mutation) CoreHash() string {
	digest := NewDigestBuffer()
	digest.WriteString(mut.Name)
	digest.AddStatements(mut.Core...)
	return digest.Digest()
}

func (mut *Mutation) GlueHash() string {
	digest := NewDigestBuffer()
	digest.WriteString(mut.Name)
	digest.AddStatements(mut.Glue...)
	return digest.Digest()
}

func (mut *Mutation) Hash(dir IterationDirection) string {
	if dir.Meta {
		return mut.GlueHash()
	} else {
		return mut.CoreHash()
	}
}

// normalizedStatement is a code-equivalence fingerprint of a single statement's up and
// down text (whitespace/comments ignored, per DigestBuffer.AddStatement), suitable for
// direct comparison between two statements - see commonPrefixLen.
func normalizedStatement(stmt MutationStatement) string {
	dg := NewDigestBuffer()
	_ = dg.AddStatement(stmt.Up)
	_ = dg.AddStatement(stmt.Down)
	return dg.String()
}

// commonPrefixLen returns the length of the longest common prefix of old and new,
// comparing statements by code-equivalence (see normalizedStatement) rather than exact
// string identity, and stopping at the first divergence - not a general diff. This is
// deliberate: everything after the first point of divergence is swept into scope even if
// individually unchanged, since a later statement can depend on what an earlier one
// produced (see specs/changes.md, "New algorithm").
func commonPrefixLen(old, new []MutationStatement) int {
	n := len(old)
	if len(new) < n {
		n = len(new)
	}
	i := 0
	for i < n && normalizedStatement(old[i]) == normalizedStatement(new[i]) {
		i++
	}
	return i
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

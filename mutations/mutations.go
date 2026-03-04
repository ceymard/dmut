package mutations

import (
	"iter"

	au "github.com/logrusorgru/aurora"
	"github.com/ugurcsen/gods-generic/sets/hashset"
)

type IterationDirection struct {
	Down bool
	Meta bool
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
	set *MutationSet

	Name      string `yaml:"-"`
	File      string `yaml:"file,omitempty"`
	Namespace string

	Needs     []string            `yaml:"needs,omitempty,flow"`
	Sql       []MutationStatement `yaml:"sql,omitempty"`
	MetaNeeds []string            `yaml:"meta_needs,omitempty,flow"`
	Meta      []MutationStatement `yaml:"meta,omitempty"`

	Roles []string `yaml:"roles,omitempty"`

	// Will only be used when loading from yaml, not from database
	SqlParents   *hashset.Set[*Mutation]
	SqlChildren  *hashset.Set[*Mutation]
	MetaParents  *hashset.Set[*Mutation]
	MetaChildren *hashset.Set[*Mutation]

	// Only used during yaml parsing
	ChildrenMutations MutationMap `yaml:"children,omitempty"`
}

func (mut *Mutation) DisplayName() string {
	return au.BrightMagenta("["+mut.set.Namespace+"]").String() + " " + mut.Name
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

func (yml *Mutation) AddRoles(roles []string) {
	for _, role := range roles {
		yml.Roles = append(yml.Roles, role)
	}
}

// Normalize the children
func (mut *Mutation) Normalize() error {
	mut.SqlParents = hashset.New[*Mutation]()
	mut.SqlChildren = hashset.New[*Mutation]()
	mut.MetaParents = hashset.New[*Mutation]()
	mut.MetaChildren = hashset.New[*Mutation]()

	for _, child := range mut.ChildrenMutations {
		child.File = mut.File
		child.Name = mut.Name + "." + child.Name
		mut.set.AddMutation(child)
	}
	return nil
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

func (mut *Mutation) RunRecursively(runner Runner, dir IterationDirection) error {
	for dep := range mut.IterateDependencies(dir) {
		if err := runner.Run(dep.Runnable(dir)); err != nil {
			return err
		}
	}
	return nil
}

package mutations

import (
	"fmt"

	mapset "github.com/deckarep/golang-set/v2"
	au "github.com/logrusorgru/aurora"
)

type DbMutation struct {
	Hash     string   `db:"hash"`
	Name     string   `db:"name"` // name is just for show
	File     string   `db:"file"`
	Meta     bool     `db:"meta"`
	Up       []string `db:"up"`
	Down     []string `db:"down"`
	Children []string `db:"children"` // hashes of the children that depend upon this mutation
	Parents  []string `db:"parents"`  // hashes of the parents that this mutation depends upon
}

func (mut *DbMutation) HasStatements() bool {
	return len(mut.Up) > 0 || len(mut.Down) > 0
}

func (mut *DbMutation) ComputeHash() {
	digest := NewDigestBuffer()
	digest.WriteString(mut.Name)
	digest.AddStatements(mut.Up...)
	digest.AddStatements(mut.Down...)
	mut.Hash = digest.Digest()
}

func (mut *DbMutation) DisplayName() string {
	meta := au.BrightMagenta("sql").String()
	if mut.Meta {
		meta = au.BrightBlue("meta").String()
	}
	return mut.Name + " " + meta
}

// Add a relationship between this mutation and the given parent mutation only if both have statements.
func (mut *DbMutation) AddParent(parent *DbMutation) {
	if parent.HasStatements() && mut.HasStatements() {
		mut.Parents = append(mut.Parents, parent.Hash)
		parent.Children = append(parent.Children, mut.Hash)
	}
}

type DbMutationMap map[string]*DbMutation

func NewDbMutationMap(mutations []*DbMutation) DbMutationMap {
	var res = make(DbMutationMap)
	for _, mut := range mutations {
		res[mut.Hash] = mut
	}
	return res
}

func (m DbMutationMap) Copy() DbMutationMap {
	var res = make(DbMutationMap)
	for _, mut := range m {
		res[mut.Hash] = mut
	}
	return res
}

func (m DbMutationMap) Remove(mut *DbMutation) {
	delete(m, mut.Hash)
}

func (m DbMutationMap) AddMutation(mut *DbMutation) {
	if _, ok := m[mut.Hash]; ok {
		panic(fmt.Errorf("mutation %s already in map, this should not happen", mut.Hash))
	}
	m[mut.Hash] = mut
}

// Get all mutations related to the given hashes in order, where applying them in this order will satisfy all dependencies
// If no hashes are given, all mutations are returned.
func (m DbMutationMap) GetMutationsInOrder(parents_first bool, hashes ...string) []*DbMutation {
	var res []*DbMutation = make([]*DbMutation, 0)
	var seen = mapset.NewSet[string]()

	var add func(mut *DbMutation)
	add = func(mut *DbMutation) {
		if seen.Contains(mut.Hash) {
			return
		}
		seen.Add(mut.Hash)
		if parents_first {
			for _, parent := range mut.Parents {
				add(m[parent])
			}
			res = append(res, mut)
		} else {
			for _, child := range mut.Children {
				add(m[child])
			}
			res = append(res, mut)
		}
	}

	if len(hashes) > 0 {
		for _, hash := range hashes {
			if mut, ok := m[hash]; ok {
				add(mut)
			}
		}
	} else {
		for _, mut := range m {
			add(mut)
		}
	}

	return res
}

func (m DbMutationMap) GetLeafMutations() []*DbMutation {
	var res []*DbMutation = make([]*DbMutation, 0)
	for _, mut := range m {
		if len(mut.Children) == 0 {
			res = append(res, mut)
		}
	}
	return res
}

// Get the hashes of the mutations that are in this map but not in the other map.
func (m DbMutationMap) GetUniqueHashes(other DbMutationMap) mapset.Set[string] {
	return mapset.NewSetFromMapKeys(m).Difference(mapset.NewSetFromMapKeys(other))
}

var dmut_mutation *DbMutation

func init() {
	dmut_mutation = &DbMutation{
		Name: "__dmut__",
		Up: []string{
			"CREATE SCHEMA dmut;",
			"CREATE TABLE dmut.mutations (hash TEXT PRIMARY KEY, name TEXT, up TEXT[], down TEXT[], children TEXT[], parents TEXT[]);",
			"CREATE TABLE dmut.roles (name TEXT PRIMARY KEY);",
		},
		Down: []string{
			"DROP TABLE dmut.mutations;",
			"DROP TABLE dmut.roles;",
			"DROP SCHEMA dmut;",
		},
	}
	dmut_mutation.ComputeHash()
}

package mutations

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"sort"
	"strings"

	dmutparser "github.com/ceymard/dmut/parser"
)

//NewDigestBuffer returns a new DigestBuffer
func NewDigestBuffer(bf []byte) *DigestBuffer {
	var buf = bytes.NewBuffer(bf)
	return &DigestBuffer{*buf}
}

//DigestBuffer is just a wrapper for bytes.Buffer
type DigestBuffer struct {
	bytes.Buffer
}

// AddStatement Adds an SQL statement to the buffer for later hash computing
// and does so by "simplifying" it, ignoring white space where convenient, but
// not inside strings or string like constructs like $$ ... $$
func (dg *DigestBuffer) AddStatement(stmt string) {
	stmt = "   " + stmt
	var reader = strings.NewReader(stmt)

	// not checking err since "normally" only code previously lexed
	// is analyzed here
	var lx, _ = dmutparser.SqlLexer.Lex("", reader)
	for tk, err := lx.Next(); !tk.EOF(); tk, err = lx.Next() {
		if err != nil {
			panic(fmt.Errorf("shouldn't happen on %s : %w", stmt, err))
		}
		_, _ = dg.WriteString(tk.String())
		_ = dg.WriteByte(' ')
	}
}

//Digest computes the SHA256 sum of what was written so far
func (dg *DigestBuffer) Digest() []byte {
	var res = sha256.Sum256(dg.Bytes())
	return res[:]
}

type MutationSet map[string]*Mutation

func (ms *MutationSet) Add(mut *Mutation) *MutationSet {
	if val, ok := (*ms)[mut.Name]; ok {
		// FIXME should be an error.
		panic(fmt.Errorf("mutation %s already in set", val.Name))
	}
	(*ms)[mut.Name] = mut
	return ms
}

func (ms *MutationSet) Delete(mut *Mutation) *MutationSet {
	delete(*ms, mut.Name)
	return ms
}

func (ms *MutationSet) GetInOrder() []*Mutation {
	var (
		res  []*Mutation
		seen = make(map[string]struct{})
		do   func(mut *Mutation)
	)

	do = func(mut *Mutation) {
		if _, ok := seen[mut.Name]; ok {
			return
		} else {
			seen[mut.Name] = struct{}{}
		}

		for _, m := range mut.Parents {
			do(m)
		}

		res = append(res, mut)
	}

	for _, mut := range *ms {
		do(mut)
	}

	return res
}

type Mutation struct {
	Name        string
	Parents     MutationSet
	Children    MutationSet
	DependsOn   *[]string
	Up          []string
	Down        []string
	HashLock    *[]byte
	hash        []byte
	hashIsStale bool
}

type Mutations []*Mutation

func (ms Mutations) Len() int {
	return len(ms)
}

func (ms Mutations) Swap(i, j int) {
	ms[i], ms[j] = ms[j], ms[i]
}

func (ms Mutations) Less(i, j int) bool {
	return ms[i].Name < ms[j].Name
}

func NewMutation(name string, dependsOn *[]string, hashLock *[]byte) *Mutation {
	return &Mutation{
		Name:        name,
		Parents:     make(MutationSet),
		Children:    make(MutationSet),
		Up:          make([]string, 0, 16),
		Down:        make([]string, 0, 16),
		DependsOn:   dependsOn,
		HashLock:    hashLock,
		hash:        nil,
		hashIsStale: true,
	}
}

func (mut *Mutation) GetParents() []*Mutation {
	var res Mutations
	for _, m := range mut.Parents {
		res = append(res, m)
	}
	sort.Sort(res)
	return res
}

func (mut *Mutation) GetParentNames() []string {
	var res []string = make([]string, 0)
	for _, m := range mut.GetParents() {
		res = append(res, m.Name)
	}
	return res
}

func (mut *Mutation) GetChildrenNames() []string {
	var res []string = make([]string, 0)
	for _, m := range mut.Children {
		res = append(res, m.Name)
	}
	return res
}

func (mut *Mutation) Lock(lock string) *Mutation {
	var lk = []byte(lock)
	mut.HashLock = &lk
	return mut
}

func (mut *Mutation) Hash() []byte {
	if mut.hash != nil && !mut.hashIsStale {
		return mut.hash
	}

	var buffer = NewDigestBuffer(make([]byte, 0, 1024))

	for _, parent := range mut.GetParents() {
		_, _ = buffer.Write(parent.Hash())
	}

	for _, up := range mut.Up {
		buffer.AddStatement(up)
	}

	for _, down := range mut.Down {
		buffer.AddStatement(down)
	}

	mut.hash = buffer.Digest()
	return mut.hash
}

func (mut *Mutation) AddParent(parent *Mutation) *Mutation {
	mut.hashIsStale = true
	mut.Parents.Add(parent)
	parent.Children.Add(mut)
	return mut
}

func (mut *Mutation) RemoveParent(parent *Mutation) *Mutation {
	mut.hashIsStale = true
	mut.Parents.Delete(parent)
	parent.Children.Delete(mut)
	return mut
}

func (mut *Mutation) AddUp(up string) *Mutation {
	if up != "" {
		mut.hashIsStale = true
		mut.Up = append(mut.Up, strings.TrimSpace(up))
	}
	return mut
}

func (mut *Mutation) AddDown(down string) *Mutation {
	if down != "" {
		mut.hashIsStale = true
		mut.Down = append([]string{strings.TrimSpace(down)}, mut.Down...)
	}
	return mut
}

func reorderMutations(muts []*Mutation) []*Mutation {
	var (
		mp  = make(map[string]*Mutation)
		res = make([]*Mutation, 0, len(muts))
		add func(m *Mutation)
	)
	add = func(m *Mutation) {
		// do not process a mutation that already was added
		if _, ok := mp[m.Name]; ok {
			return
		}
		for _, p := range m.Parents {
			add(p)
		}
		res = append(res, m)
	}

	for _, m := range muts {
		add(m)
	}

	return res
}

///////////////

func first(args ...string) string {
	for _, s := range args {
		if s != "" {
			return s
		}
	}
	return ""
}

var (
	dmutSchema   = first(os.Getenv("DMUT_SCHEMA"), "dmut")
	DmutMutation = NewMutation(
		"dmut.base",
		nil,
		nil,
	).AddDown(
		fmt.Sprintf(`DROP SCHEMA "%s";`, dmutSchema),
	).AddUp(
		fmt.Sprintf(`CREATE SCHEMA "%s";`, dmutSchema),
	).AddDown(
		fmt.Sprintf(`DROP TABLE "%s".mutations`, dmutSchema),
	).AddUp(
		fmt.Sprintf(`
		CREATE TABLE "%s".mutations (
			"hash" TEXT PRIMARY KEY NOT NULL,
			"name" TEXT NOT NULL,
			"up" TEXT[] NOT NULL,
			"down" TEXT[] NOT NULL,
			"children" TEXT[] NOT NULL,
			"date_applied" TIMESTAMP DEFAULT NOW()
		);
		`, dmutSchema),
	)

	DmutMutations = []*Mutation{DmutMutation}
)

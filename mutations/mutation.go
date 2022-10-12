package mutations

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
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
func (dg *DigestBuffer) AddStatement(stmt string) error {
	stmt = "   " + stmt
	var reader = strings.NewReader(stmt)

	// not checking err since "normally" only code previously lexed
	// is analyzed here
	var lx, _ = dmutparser.SqlLexer.Lex("", reader)
	for tk, err := lx.Next(); !tk.EOF(); tk, err = lx.Next() {
		if err != nil {
			log.Print(stmt)
			return err
		}
		_, _ = dg.WriteString(tk.String())
		_ = dg.WriteByte(' ')
	}
	return nil
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
		}
		seen[mut.Name] = struct{}{}

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
	File        string
	Name        string
	Parents     MutationSet
	Children    MutationSet
	DependsOn   []string
	Up          []string
	Down        []string
	HashLock    *[]byte
	Hash        string
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

func NewMutation(filename string, name string, dependsOn []string, hashLock *[]byte) *Mutation {
	return &Mutation{
		File:        filename,
		Name:        name,
		Parents:     make(MutationSet),
		Children:    make(MutationSet),
		Up:          make([]string, 0, 16),
		Down:        make([]string, 0, 16),
		DependsOn:   dependsOn,
		HashLock:    hashLock,
		Hash:        "",
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
	var res = make([]string, 0)
	for _, m := range mut.GetParents() {
		res = append(res, m.Name)
	}
	return res
}

func (mut *Mutation) GetChildrenNames() []string {
	var res = make([]string, 0)
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

func (mut *Mutation) ComputeHash() (string, error) {
	var err error

	if mut.Hash != "" && !mut.hashIsStale {
		return mut.Hash, nil
	}

	var buffer = NewDigestBuffer(make([]byte, 0, 1024))

	for _, parent := range mut.GetParents() {
		hash, err := parent.ComputeHash()
		if err != nil {
			return "", err
		}
		_, _ = buffer.WriteString(hash)
	}

	for _, up := range mut.Up {
		if err = buffer.AddStatement(up); err != nil {
			return "", err
		}
	}

	for _, down := range mut.Down {
		if err = buffer.AddStatement(down); err != nil {
			return "", err
		}
	}

	mut.Hash = hex.EncodeToString(buffer.Digest())
	return mut.Hash, nil
}

func (mut *Mutation) AddParent(parent *Mutation) *Mutation {
	if _, ok := mut.Parents[parent.Name]; ok {
		return mut
	}
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
		mp[m.Name] = m
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
		"--base--",
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

	DmutSqliteMutation = NewMutation(
		"--base--",
		"dmut.base",
		nil,
		nil,
	).AddDown(`DROP TABLE _dmut_mutations`).AddUp(`
		CREATE TABLE _dmut_mutations (
			"hash" TEXT PRIMARY KEY NOT NULL,
			"name" TEXT NOT NULL,
			"up" JSON NOT NULL,
			"down" JSON NOT NULL,
			"children" JSON NOT NULL,
			"date_applied" TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)

	DmutMutations       = []*Mutation{DmutMutation}
	DmutSqliteMutations = []*Mutation{DmutSqliteMutation}
)

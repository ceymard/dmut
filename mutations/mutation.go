package mutations

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"

	"github.com/alecthomas/participle/lexer"
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
	var reader = strings.NewReader(stmt)

	// not checking err since "normally" only code previously lexed
	// is analyzed here
	var lx, _ = dmutparser.SqlLexer.Lex(reader)
	for tk, _ := lx.Next(); tk.Type != lexer.EOF; tk, _ = lx.Next() {
		dg.WriteString(tk.Value)
		dg.WriteByte(' ')
	}
}

//Digest computes the SHA256 sum of what was written so far
func (dg *DigestBuffer) Digest() []byte {
	var res = sha256.Sum256(dg.Bytes())
	return res[:]
}

type MutationSet map[string]*Mutation

func (ms *MutationSet) Add(mut *Mutation) *MutationSet {
	(*ms)[mut.Name] = mut
	return ms
}

func (ms *MutationSet) Delete(mut *Mutation) *MutationSet {
	delete(*ms, mut.Name)
	return ms
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

	for _, parent := range mut.Parents {
		buffer.Write(parent.Hash())
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
		mut.Down = append(mut.Down, strings.TrimSpace(down))
	}
	return mut
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
	DMUT_SCHEMA  = first(os.Getenv("DMUT_SCHEMA"), "dmut")
	DmutMutation = NewMutation(
		"dmut.base",
		nil,
		nil,
	).AddDown(
		fmt.Sprintf(`DROP SCHEMA "%s";`, DMUT_SCHEMA),
	).AddUp(
		fmt.Sprintf(`CREATE SCHEMA "%s";`, DMUT_SCHEMA),
	).AddDown(
		fmt.Sprintf(`DROP TABLE "%s".mutations`, DMUT_SCHEMA),
	).AddUp(
		fmt.Sprintf(`
		CREATE TABLE "%s".mutations (
			"hash" TEXT PRIMARY KEY NOT NULL,
			"namespace" TEXT,
			"identifier" TEXT NOT NULL,
			"statements" TEXT[] NOT NULL,
			"undo" TEXT[] NOT NULL,
			"parents" TEXT[] NOT NULL,
			"date_applied" TIMESTAMP DEFAULT NOW()
		);
		`, DMUT_SCHEMA),
	)
)

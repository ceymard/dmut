package mutations

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"log"
	"strings"
)

// NewDigestBuffer returns a new DigestBuffer
func NewDigestBuffer() *DigestBuffer {
	var buf = bytes.NewBuffer(make([]byte, 0, 1024))
	return &DigestBuffer{*buf}
}

// DigestBuffer is just a wrapper for bytes.Buffer
type DigestBuffer struct {
	bytes.Buffer
}

// AddStatement Adds an SQL statement to the buffer for later hash computing
// and does so by "simplifying" it, ignoring white space where convenient, but
// not inside strings or string like constructs like $$ ... $$
func (dg *DigestBuffer) AddStatement(stmt string) error {
	stmt = "   " + stmt
	// var reader = strings.NewReader(stmt)

	// not checking err since "normally" only code previously lexed
	// is analyzed here
	var lx, _ = SqlLexer.Lex("", strings.NewReader(stmt))
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

func (dg *DigestBuffer) AddStatements(stmts ...string) error {
	for _, stmt := range stmts {
		if err := dg.AddStatement(stmt); err != nil {
			return err
		}
	}
	return nil
}

// Digest computes the SHA256 sum of what was written so far
func (dg *DigestBuffer) Digest() string {
	var res = sha256.Sum256(dg.Bytes())
	return base64.StdEncoding.EncodeToString(res[:])
}

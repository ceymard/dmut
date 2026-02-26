package mutations

import (
	"strings"
	"testing"

	lexer "github.com/alecthomas/participle/v2/lexer"
	"github.com/k0kubun/pp"
)

func compareIdToken(cmp lexer.Token, got lexer.Token) bool {
	if len(cmp.Value) > 0 && cmp.Value[0] == '"' || len(got.Value) > 0 && got.Value[0] == '"' {
		return cmp.Value == got.Value
	}
	return strings.EqualFold(cmp.Value, got.Value)
}

func compareTokens(t *testing.T, id_token lexer.TokenType, cmp string, got string) bool {
	cmpTokens, err := split(cmp)
	if err != nil {
		return false
	}
	gotTokens, err := split(got)
	if err != nil {
		return false
	}
	if len(cmpTokens) != len(gotTokens) {
		pp.Println("token count mismatch", len(cmpTokens), len(gotTokens))
		return false
	}
	for i := range cmpTokens {

		if cmpTokens[i].Type == id_token {
			// ids are case insensitive
			if !compareIdToken(cmpTokens[i], gotTokens[i]) {

				return false
			}
		} else {
			if cmpTokens[i].Value != gotTokens[i].Value {

				return false
			}
		}
	}
	return true
}

func TestSplit(t *testing.T) {

	tests := []struct {
		upSQL    string
		wantDown string
	}{
		// SimpleCreateStatement: TABLE, VIEW, MATERIALIZED VIEW, EXTENSION, SCHEMA, TYPE, ROLE
		{
			upSQL:    "CREATE TABLE foo (id int);",
			wantDown: "DROP TABLE foo;",
		},
		{
			upSQL:    "CREATE TABLE public.bar (a text);",
			wantDown: "DROP TABLE public.bar;",
		},
		{
			upSQL:    "CREATE VIEW v AS SELECT 1;",
			wantDown: "DROP VIEW v;",
		},
		{
			upSQL:    "CREATE MATERIALIZED VIEW mv AS SELECT 1;",
			wantDown: "DROP MATERIALIZED VIEW mv;",
		},
		{
			upSQL:    "CREATE EXTENSION pgcrypto;",
			wantDown: "DROP EXTENSION pgcrypto;",
		},
		{
			upSQL:    "CREATE SCHEMA app;",
			wantDown: "DROP SCHEMA app;",
		},
		{
			upSQL:    "CREATE TYPE status AS ENUM ('a', 'b');",
			wantDown: "DROP TYPE status;",
		},
		{
			upSQL:    "CREATE ROLE myrole;",
			wantDown: "DROP ROLE myrole;",
		},
		// CreateIndexStatement
		{
			upSQL:    "CREATE INDEX idx ON t (col);",
			wantDown: "DROP INDEX idx;",
		},
		{
			upSQL:    "CREATE UNIQUE INDEX idx ON t (col);",
			wantDown: "DROP INDEX idx;",
		},
		{
			upSQL:    "CREATE INDEX CONCURRENTLY idx ON t (col);",
			wantDown: "DROP INDEX idx;",
		},
		{
			upSQL:    "CREATE INDEX idx ON myschema.t (col);",
			wantDown: "DROP INDEX idx;",
		},
		{
			upSQL:    "CREATE FUNCTION f() RETURNS int AS $$ SELECT 1 $$ LANGUAGE sql;",
			wantDown: "DROP FUNCTION f();",
		},
		{
			upSQL:    "CREATE FUNCTION f(integer) RETURNS int AS $f$ SELECT 1 $f$ LANGUAGE sql;",
			wantDown: "DROP FUNCTION f(integer);",
		},
		{
			upSQL:    "CREATE FUNCTION f(a1 integer, a2 text) RETURNS void AS $$ $$ LANGUAGE sql;",
			wantDown: "DROP FUNCTION f(a1 integer, a2 text);",
		},
		{
			upSQL:    "CREATE FUNCTION f(a1 integer, a2 text DEFAULT 'foo') RETURNS void AS $$ $$ LANGUAGE sql;",
			wantDown: "DROP FUNCTION f(a1 integer, a2 text);",
		},
		// CreatePolicyOrTriggerStmt
		{
			upSQL:    "CREATE POLICY p ON t USING (true);",
			wantDown: "DROP POLICY p ON t;",
		},
		{
			upSQL:    "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f();",
			wantDown: "DROP TRIGGER tr ON t;",
		},
		// GrantStatement
		{
			upSQL:    "GRANT SELECT ON table t TO u;",
			wantDown: "REVOKE SELECT ON table t FROM u;",
		},
		{
			upSQL:    "GRANT SELECT, INSERT, UPDATE ON table t TO u;",
			wantDown: "REVOKE SELECT, INSERT, UPDATE ON table t FROM u;",
		},
		{
			upSQL:    "GRANT USAGE ON schema s TO u;",
			wantDown: "REVOKE USAGE ON schema s FROM u;",
		},
		{
			upSQL:    "GRANT USAGE ON sequence seq TO u;",
			wantDown: "REVOKE USAGE ON sequence seq FROM u;",
		},
		{
			upSQL:    `GRANT "admin" TO "user1", "user2";`,
			wantDown: `REVOKE "admin" FROM "user1", "user2";`,
		},
		// CommentStatement (no undo)
		{
			upSQL:    "COMMENT ON table t IS 'comment';",
			wantDown: "",
		},
		// AlterTableStmt variants
		{
			upSQL:    "ALTER TABLE t ENABLE ROW LEVEL SECURITY;",
			wantDown: "ALTER TABLE t DISABLE ROW LEVEL SECURITY;",
		},
		{
			upSQL:    "ALTER TABLE t ADD COLUMN c integer;",
			wantDown: "ALTER TABLE t DROP COLUMN c;",
		},
		{
			upSQL:    "ALTER TABLE t ADD COLUMN c integer DEFAULT 0;",
			wantDown: "ALTER TABLE t DROP COLUMN c;",
		},
		{
			upSQL:    "ALTER TABLE t ALTER COLUMN c SET DEFAULT 1;",
			wantDown: "ALTER TABLE t ALTER COLUMN c DROP DEFAULT;",
		},
		{
			upSQL:    "ALTER TABLE t ADD CONSTRAINT ck CHECK (x > 0);",
			wantDown: "ALTER TABLE t DROP CONSTRAINT ck;",
		},
		{
			upSQL:    "ALTER TABLE t ADD CONSTRAINT t_pkey PRIMARY KEY (id);",
			wantDown: "ALTER TABLE t DROP CONSTRAINT t_pkey;",
		},
		// CREATE OPERATOR / DROP OPERATOR (https://www.postgresql.org/docs/18/sql-createoperator.html)
		{
			upSQL:    "CREATE OPERATOR === (LEFTARG = box, RIGHTARG = box, FUNCTION = area_equal_function);",
			wantDown: "DROP OPERATOR === (box, box);",
		},
		{
			upSQL:    "CREATE OPERATOR + (LEFTARG = int4, RIGHTARG = int4, FUNCTION = int4pl);",
			wantDown: "DROP OPERATOR + (int4, int4);",
		},
		{
			upSQL:    "CREATE OPERATOR @ (RIGHTARG = mytype, FUNCTION = mytype_negate);",
			wantDown: "DROP OPERATOR @ (NONE, mytype);",
		},
		{
			upSQL:    "CREATE OPERATOR myschema.=== (LEFTARG = box, RIGHTARG = box, FUNCTION = area_equal_function, COMMUTATOR = ===, NEGATOR = !==);",
			wantDown: "DROP OPERATOR myschema.=== (box, box);",
		},
	}

	id_token := SqlLexer.Symbols()["Id"]
	for _, tt := range tests {
		t.Run(tt.upSQL, func(t *testing.T) {
			got, err := AutoDowner.ParseAndGetDefault(tt.upSQL)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			if !compareTokens(t, id_token, got, tt.wantDown) {
				t.Errorf("Down() = %q, want %q", got, tt.wantDown)
			}
		})
	}
}

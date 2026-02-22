package mutations

import (
	"testing"
)

func TestDownGeneration(t *testing.T) {
	tests := []struct {
		name     string
		upSQL    string
		wantDown string
	}{
		// SimpleCreateStatement: TABLE, VIEW, MATERIALIZED VIEW, EXTENSION, SCHEMA, TYPE, ROLE
		{
			name:     "CREATE TABLE",
			upSQL:    "CREATE TABLE foo (id int);",
			wantDown: "DROP TABLE foo;",
		},
		{
			name:     "CREATE TABLE schema-qualified",
			upSQL:    "CREATE TABLE public.bar (a text);",
			wantDown: "DROP TABLE public.bar;",
		},
		{
			name:     "CREATE VIEW",
			upSQL:    "CREATE VIEW v AS SELECT 1;",
			wantDown: "DROP VIEW v;",
		},
		{
			name:     "CREATE MATERIALIZED VIEW",
			upSQL:    "CREATE MATERIALIZED VIEW mv AS SELECT 1;",
			wantDown: "DROP MATERIALIZED VIEW mv;",
		},
		{
			name:     "CREATE EXTENSION",
			upSQL:    "CREATE EXTENSION pgcrypto;",
			wantDown: "DROP EXTENSION pgcrypto;",
		},
		{
			name:     "CREATE SCHEMA",
			upSQL:    "CREATE SCHEMA app;",
			wantDown: "DROP SCHEMA app;",
		},
		{
			name:     "CREATE TYPE",
			upSQL:    "CREATE TYPE status AS ENUM ('a', 'b');",
			wantDown: "DROP TYPE status;",
		},
		{
			name:     "CREATE ROLE",
			upSQL:    "CREATE ROLE myrole;",
			wantDown: "DROP ROLE myrole;",
		},
		// CreateIndexStatement
		{
			name:     "CREATE INDEX",
			upSQL:    "CREATE INDEX idx ON t (col);",
			wantDown: "DROP INDEX idx;",
		},
		{
			name:     "CREATE UNIQUE INDEX",
			upSQL:    "CREATE UNIQUE INDEX idx ON t (col);",
			wantDown: "DROP INDEX idx;",
		},
		{
			name:     "CREATE INDEX CONCURRENTLY",
			upSQL:    "CREATE INDEX CONCURRENTLY idx ON t (col);",
			wantDown: "DROP INDEX idx;",
		},
		{
			name:     "CREATE INDEX schema-qualified table",
			upSQL:    "CREATE INDEX idx ON myschema.t (col);",
			wantDown: "DROP INDEX myschema.idx;",
		},
		// CreateFunctionStatement
		{
			name:     "CREATE FUNCTION",
			upSQL:    "CREATE FUNCTION f(integer) RETURNS int AS $f$ SELECT 1 $f$ LANGUAGE sql;",
			wantDown: "DROP FUNCTION f (integer);",
		},
		{
			name:     "CREATE FUNCTION multiple args",
			upSQL:    "CREATE FUNCTION f(a1 integer, a2 text) RETURNS void AS $$ $$ LANGUAGE sql;",
			wantDown: "DROP FUNCTION f (a1 integer , a2 text);",
		},
		{
			name:     "CREATE OR REPLACE FUNCTION",
			upSQL:    "CREATE OR REPLACE FUNCTION f() RETURNS int AS $$ SELECT 1 $$;",
			wantDown: "DROP FUNCTION f ();",
		},
		// CreatePolicyOrTriggerStmt
		{
			name:     "CREATE POLICY",
			upSQL:    "CREATE POLICY p ON t USING (true);",
			wantDown: "DROP POLICY p ON t;",
		},
		{
			name:     "CREATE TRIGGER",
			upSQL:    "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION f();",
			wantDown: "DROP TRIGGER tr ON t;",
		},
		// GrantStatement
		{
			name:     "GRANT on table",
			upSQL:    "GRANT SELECT ON table t TO u;",
			wantDown: "REVOKE SELECT ON table t FROM u;",
		},
		{
			name:     "GRANT multiple perms",
			upSQL:    "GRANT SELECT, INSERT, UPDATE ON table t TO u;",
			wantDown: "REVOKE SELECT , INSERT , UPDATE ON table t FROM u;",
		},
		{
			name:     "GRANT on schema",
			upSQL:    "GRANT USAGE ON schema s TO u;",
			wantDown: "REVOKE USAGE ON schema s FROM u;",
		},
		{
			name:     "GRANT on sequence",
			upSQL:    "GRANT USAGE ON sequence seq TO u;",
			wantDown: "REVOKE USAGE ON sequence seq FROM u;",
		},
		{
			name:     "GRANT ROLE to other",
			upSQL:    `GRANT "admin" TO "user1", "user2";`,
			wantDown: `REVOKE "admin" FROM "user1", "user2";`,
		},
		// CommentStatement (no undo)
		{
			name:     "COMMENT ON",
			upSQL:    "COMMENT ON table t IS 'comment';",
			wantDown: "",
		},
		// AlterTableStmt variants
		{
			name:     "ALTER TABLE ENABLE ROW LEVEL SECURITY",
			upSQL:    "ALTER TABLE t ENABLE ROW LEVEL SECURITY;",
			wantDown: "ALTER TABLE t DISABLE ROW LEVEL SECURITY;",
		},
		{
			name:     "ALTER TABLE ADD COLUMN",
			upSQL:    "ALTER TABLE t ADD COLUMN c integer;",
			wantDown: "ALTER TABLE t DROP COLUMN c;",
		},
		{
			name:     "ALTER TABLE ADD COLUMN with default",
			upSQL:    "ALTER TABLE t ADD COLUMN c integer DEFAULT 0;",
			wantDown: "ALTER TABLE t DROP COLUMN c;",
		},
		{
			name:     "ALTER TABLE ALTER COLUMN SET DEFAULT",
			upSQL:    "ALTER TABLE t ALTER COLUMN c SET DEFAULT 1;",
			wantDown: "ALTER TABLE t ALTER COLUMN c DROP DEFAULT;",
		},
		{
			name:     "ALTER TABLE ADD CONSTRAINT",
			upSQL:    "ALTER TABLE t ADD CONSTRAINT ck CHECK (x > 0);",
			wantDown: "ALTER TABLE t DROP CONSTRAINT ck;",
		},
		{
			name:     "ALTER TABLE ADD CONSTRAINT primary key",
			upSQL:    "ALTER TABLE t ADD CONSTRAINT t_pkey PRIMARY KEY (id);",
			wantDown: "ALTER TABLE t DROP CONSTRAINT t_pkey;",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parser.ParseString("", tt.upSQL)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			down := (*got).Down()
			if down != tt.wantDown {
				t.Errorf("Down() = %q, want %q", down, tt.wantDown)
			}
		})
	}
}

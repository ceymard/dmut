package mutations

import "testing"

// Statements that have no reverse must still be writable as a plain string :
// "no down" is a valid outcome, unlike "we could not find a down".
func TestStatementsWithoutDown(t *testing.T) {
	for _, up := range []string{
		"comment on table api.users is 'application users';",
		"COMMENT ON COLUMN api.users.id IS 'primary key';",
	} {
		stmt, err := mutationStatementFromString(up)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", up, err)
			continue
		}
		if stmt.Up != up {
			t.Errorf("%s: up was rewritten to %q", up, stmt.Up)
		}
		if stmt.Down != "" {
			t.Errorf("%s: expected no down, got %q", up, stmt.Down)
		}
	}
}

// A statement dmut does not understand must still be refused, rather than
// silently getting an empty down.
func TestUnknownStatementIsRefused(t *testing.T) {
	for _, up := range []string{
		"drop table if exists t;",
		"update api.users set name = 'x';",
	} {
		if _, err := mutationStatementFromString(up); err == nil {
			t.Errorf("%s: expected an error, got none", up)
		}
	}
}

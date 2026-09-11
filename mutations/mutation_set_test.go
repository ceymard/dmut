package mutations

import "testing"

func stmt(up string) MutationStatement {
	return MutationStatement{Up: up, Down: ""}
}

func stmts(ups ...string) []MutationStatement {
	var res []MutationStatement
	for _, up := range ups {
		res = append(res, stmt(up))
	}
	return res
}

func TestCommonPrefixLenIdenticalLists(t *testing.T) {
	old := stmts("create table t (id int);", "alter table t add column c text;")
	new_ := stmts("create table t (id int);", "alter table t add column c text;")

	if got := commonPrefixLen(old, new_); got != 2 {
		t.Errorf("expected the whole list to match, got prefix length %d", got)
	}
}

// A pure append (nothing before it changed) must not be swept into a down/up tail -
// this is the whole point of the longest-common-prefix diff (see specs/changes.md,
// "Why").
func TestCommonPrefixLenPureAppend(t *testing.T) {
	old := stmts("create table t (id int);")
	new_ := stmts("create table t (id int);", "create index t_idx on t (id);")

	got := commonPrefixLen(old, new_)
	if got != 1 {
		t.Errorf("expected the common prefix to cover the unchanged statement, got %d", got)
	}
	old_tail := old[got:]
	new_tail := new_[got:]
	if len(old_tail) != 0 {
		t.Errorf("a pure append must have an empty old tail, got %d statements", len(old_tail))
	}
	if len(new_tail) != 1 {
		t.Errorf("expected exactly the appended statement in the new tail, got %d", len(new_tail))
	}
}

// Editing a statement in the middle sweeps everything positionally after it into
// both tails, even statements whose own text never changed - this is required for
// correctness (see specs/changes.md, "New algorithm"), not just simplicity.
func TestCommonPrefixLenEditInTheMiddleSweepsTheRest(t *testing.T) {
	old := stmts(
		"create table t (id int);",
		"alter table t add column foo text;",
		"alter table t rename column foo to bar;",
	)
	new_ := stmts(
		"create table t (id int);",
		"alter table t add column foo2 text;",
		"alter table t rename column foo to bar;", // unedited, now stale
	)

	got := commonPrefixLen(old, new_)
	if got != 1 {
		t.Fatalf("expected the prefix to stop right before the edited statement, got %d", got)
	}
	if len(old[got:]) != 2 || len(new_[got:]) != 2 {
		t.Errorf("expected both tails to cover everything after the edit, including the "+
			"unedited rename, got old=%d new=%d", len(old[got:]), len(new_[got:]))
	}
}

// Whitespace and comments are normalized away before comparison - see DigestBuffer.
func TestCommonPrefixLenIgnoresWhitespaceAndComments(t *testing.T) {
	old := stmts("create   table t (id int);")
	new_ := stmts("-- a comment\ncreate table t (id int);\n")

	if got := commonPrefixLen(old, new_); got != 1 {
		t.Errorf("expected whitespace/comment-only differences to compare equal, got prefix length %d", got)
	}
}

func TestCommonPrefixLenEmptyLists(t *testing.T) {
	if got := commonPrefixLen(nil, nil); got != 0 {
		t.Errorf("expected 0 for two empty lists, got %d", got)
	}
	if got := commonPrefixLen(nil, stmts("create schema x;")); got != 0 {
		t.Errorf("expected 0 when there's nothing to compare against, got %d", got)
	}
}

package main

import (
	"testing"

	"github.com/ceymard/dmut/v2/mutations"
)

// A small schema with no roles in it : roles are cluster-wide and would collide
// between tests sharing the same postgres.
const basic = `api:
  sql:
    - create schema api;

api.users:
  sql:
    - |
      create table api.users (
        id serial primary key,
        name text not null
      );
  meta:
    - grant select on table api.users to public;
`

func TestApplyCreatesEverything(t *testing.T) {
	uri := testdb(t)
	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basic}))

	if !schemaExists(t, uri, "api") {
		t.Error("the api schema should have been created")
	}
	if !tableExists(t, uri, "users") {
		t.Error("the users table should have been created")
	}
	if savedMutations(t, uri, "") != 2 {
		t.Errorf("expected 2 mutations to be recorded, got %d", savedMutations(t, uri, ""))
	}
}

// Applying twice must leave the data alone : this is what tells you dmut noticed
// that nothing changed.
func TestApplyingTwiceKeepsTheData(t *testing.T) {
	uri := testdb(t)
	dir := mutationDir(t, map[string]string{"a.yml": basic})

	mustApply(t, uri, dir)
	exec(t, uri, "insert into api.users (name) values ('someone');")

	mustApply(t, uri, dir)

	if got := queryInt(t, uri, "select count(*) from api.users"); got != 1 {
		t.Errorf("expected the row to survive a second apply, found %d rows", got)
	}
}

// Changing the sql of a mutation drops and recreates the object it describes.
func TestChangingSqlLosesTheData(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basic}))
	exec(t, uri, "insert into api.users (name) values ('someone');")

	changed := `api:
  sql:
    - create schema api;

api.users:
  sql:
    - |
      create table api.users (
        id serial primary key,
        name text not null,
        email text
      );
  meta:
    - grant select on table api.users to public;
`
	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": changed}))

	if !columnExists(t, uri, "users", "email") {
		t.Error("the new column should be there")
	}
	if got := queryInt(t, uri, "select count(*) from api.users"); got != 0 {
		t.Errorf("expected the table to have been recreated, found %d rows", got)
	}
}

// Changing meta must not touch the table it applies to.
func TestChangingMetaKeepsTheData(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basic}))
	exec(t, uri, "insert into api.users (name) values ('someone');")

	changed := `api:
  sql:
    - create schema api;

api.users:
  sql:
    - |
      create table api.users (
        id serial primary key,
        name text not null
      );
  meta:
    - grant select, insert on table api.users to public;
`
	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": changed}))

	if got := queryInt(t, uri, "select count(*) from api.users"); got != 1 {
		t.Errorf("a meta change must not drop the table, found %d rows", got)
	}
}

// Removing a mutation downs it, and everything that depended on it.
func TestRemovingAMutationDownsIt(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basic}))
	mustApply(t, uri, mutationDir(t, map[string]string{
		"a.yml": "api:\n  sql:\n    - create schema api;\n",
	}))

	if tableExists(t, uri, "users") {
		t.Error("the users table should have been dropped")
	}
	if !schemaExists(t, uri, "api") {
		t.Error("the api schema should still be there")
	}
}

func TestDryRunChangesNothing(t *testing.T) {
	uri := testdb(t)

	if err := (ApplyCmd{
		Uri:   uri,
		Paths: []string{mutationDir(t, map[string]string{"a.yml": basic})},
		Dry:   true,
	}).Run(); err != nil {
		t.Fatalf("expected the dry run to succeed, got: %v", err)
	}

	if schemaExists(t, uri, "api") {
		t.Error("a dry run must not create anything")
	}
	if schemaExists(t, uri, "__dmut__") {
		t.Error("a dry run must not record anything")
	}
}

// The test phase is what makes dmut refuse mutations it could not undo.
func TestAMutationThatCannotBeDownedIsRefused(t *testing.T) {
	uri := testdb(t)

	// The down does not actually drop the table, so re-upping it fails.
	err := apply(t, uri, mutationDir(t, map[string]string{
		"a.yml": `x:
  sql:
    - up: create table lying (id int);
      down: select 1;
`,
	}))

	requireErrorContaining(t, err, "already exists")

	if schemaExists(t, uri, "__dmut__") {
		t.Error("nothing should have been committed after a failed apply")
	}
}

// Failure at any point rolls the whole thing back.
func TestABrokenStatementRollsEverythingBack(t *testing.T) {
	uri := testdb(t)

	err := apply(t, uri, mutationDir(t, map[string]string{
		"a.yml": `good:
  sql:
    - create schema good;

bad:
  needs: [good]
  sql:
    - create table good.t (id nonexistent_type);
`,
	}))
	if err == nil {
		t.Fatal("expected the apply to fail")
	}

	if schemaExists(t, uri, "good") {
		t.Error("the schema created before the failure should have been rolled back")
	}
}

// --override is for adopting objects that are already there, or replacing a
// recorded version that was wrong, without running the statements. The test phase
// still runs, so the objects must really match what is being recorded - and the
// data must survive it.
func TestOverrideAdoptsExistingObjects(t *testing.T) {
	uri := testdb(t)

	// dmut has to have its own bookkeeping in place first : see
	// TestOverrideCannotBootstrapDmut below.
	mustApply(t, uri, mutationDir(t, map[string]string{}))

	// The schema was made by hand, dmut knows nothing about it.
	exec(t, uri, "create schema api;")
	exec(t, uri, "create table api.users (id serial primary key, name text not null);")
	exec(t, uri, "grant select on table api.users to public;")
	exec(t, uri, "insert into api.users (name) values ('was here first');")

	if err := (ApplyCmd{
		Uri:      uri,
		Paths:    []string{mutationDir(t, map[string]string{"a.yml": basic})},
		Override: true,
	}).Run(); err != nil {
		t.Fatalf("expected the override to succeed, got: %v", err)
	}

	if savedMutations(t, uri, "") != 2 {
		t.Errorf("expected the mutations to be recorded, got %d", savedMutations(t, uri, ""))
	}
	if got := queryInt(t, uri, "select count(*) from api.users"); got != 1 {
		t.Errorf("the existing data must survive the test phase, found %d rows", got)
	}
}

// Recording mutations that do not match what is in the database is refused,
// because the test phase cannot down what is not there.
func TestOverrideStillHasToBeDownable(t *testing.T) {
	uri := testdb(t)
	mustApply(t, uri, mutationDir(t, map[string]string{}))

	err := (ApplyCmd{
		Uri:      uri,
		Paths:    []string{mutationDir(t, map[string]string{"a.yml": basic})},
		Override: true,
	}).Run()

	if err == nil {
		t.Fatal("expected overriding objects that are not there to fail")
	}
}

// KNOWN LIMITATION, not a wish : --override applies to every namespace, including
// the one dmut keeps its own bookkeeping in, so on a database dmut has never
// touched it records mutations into a table it just skipped creating. Run a plain
// apply once before overriding. Should this ever be fixed, this test will fail and
// should be replaced by one asserting that it works.
func TestOverrideCannotBootstrapDmut(t *testing.T) {
	uri := testdb(t)

	err := (ApplyCmd{
		Uri:      uri,
		Paths:    []string{mutationDir(t, map[string]string{"a.yml": basic})},
		Override: true,
	}).Run()

	requireErrorContaining(t, err, "__dmut__.mutations")
}

// //////////////////////////////////////////////////////////////
// Namespaces

const other_namespace = `__namespace: other

blog:
  sql:
    - create schema blog;
`

func TestNamespacesAreIndependent(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, mutationDir(t, map[string]string{
		"a.yml": basic,
		"b.yml": other_namespace,
	}))

	if !schemaExists(t, uri, "api") || !schemaExists(t, uri, "blog") {
		t.Fatal("both namespaces should have been applied")
	}

	if err := (DownCmd{Uri: uri, Namespace: "other"}).Run(); err != nil {
		t.Fatalf("expected the down to succeed, got: %v", err)
	}

	if schemaExists(t, uri, "blog") {
		t.Error("the downed namespace should be gone")
	}
	if !schemaExists(t, uri, "api") {
		t.Error("downing a namespace must not touch another one")
	}
	if savedMutations(t, uri, "other") != 0 {
		t.Error("the downed namespace should have no mutations left")
	}
	if savedMutations(t, uri, "") == 0 {
		t.Error("the other namespace should still be recorded")
	}
}

func TestDownRemovesEverything(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basic}))

	if err := (DownCmd{Uri: uri, Namespace: ""}).Run(); err != nil {
		t.Fatalf("expected the down to succeed, got: %v", err)
	}

	if schemaExists(t, uri, "api") {
		t.Error("the schema should have been dropped")
	}
	if savedMutations(t, uri, "") != 0 {
		t.Error("no mutation should be recorded any more")
	}
}

// //////////////////////////////////////////////////////////////
// Revisions

// The revision fixtures : r2 consolidates the column r1 did not have, r3 is the
// cleaned-up definition and checks that no data was lost on the way.
const revision_paths = "test/revision"

func TestEmptyDatabaseGetsOnlyTheLatestRevision(t *testing.T) {
	uri := testdb(t)
	mustApply(t, uri, revision_paths)

	if got := dbRevision(t, uri, "test"); got != 3 {
		t.Errorf("expected the database to be at revision 3, got %d", got)
	}
	if !columnExists(t, uri, "test", "age") {
		t.Error("the consolidated definition should have the age column")
	}
	// revision 3 does not define the mutation that added the column
	if savedMutations(t, uri, "test") != 1 {
		t.Errorf("expected a single mutation, got %d", savedMutations(t, uri, "test"))
	}
}

// Going through the revisions must not lose the rows that are already there.
func TestRevisionsKeepTheData(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, revision_paths+"/test-r1.yml")
	if got := dbRevision(t, uri, "test"); got != 1 {
		t.Fatalf("expected the database to be at revision 1, got %d", got)
	}
	exec(t, uri, "insert into test (name) values ('written at revision 1');")

	mustApply(t, uri, revision_paths)

	if got := dbRevision(t, uri, "test"); got != 3 {
		t.Errorf("expected the database to be at revision 3, got %d", got)
	}
	if !columnExists(t, uri, "test", "age") {
		t.Error("the column added by revision 2 should be there")
	}
	if got := queryInt(t, uri, "select count(*) from test where name = 'written at revision 1'"); got != 1 {
		t.Error("the row written before the revisions should have survived them")
	}
}

// Replaying every revision from the start is what `dmut test -a` does in CI : it
// proves that the path existing databases will take actually works, instead of
// only checking the consolidated definition.
func TestReplayingAllRevisions(t *testing.T) {
	uri := testdb(t)

	if err := mutations.ReadAndRunMutations(uri, []string{revision_paths}, &mutations.MutationRunnerOptions{
		Commit: true,
		All:    true,
	}); err != nil {
		t.Fatalf("expected the replay to succeed, got: %v", err)
	}

	if got := dbRevision(t, uri, "test"); got != 3 {
		t.Errorf("expected the database to end up at revision 3, got %d", got)
	}
	// This row is inserted by the mutation revision 2 retires, so it is only
	// there when the revisions were really replayed one after the other.
	if got := queryInt(t, uri, "select count(*) from test where name = 'test'"); got != 1 {
		t.Error("replaying every revision should have run the statements of revision 2")
	}
}

// Without -a, an empty database is seeded from the latest revision only.
func TestEmptyDatabaseSkipsOldRevisions(t *testing.T) {
	uri := testdb(t)
	mustApply(t, uri, revision_paths)

	if got := queryInt(t, uri, "select count(*) from test where name = 'test'"); got != 0 {
		t.Error("the statements of revision 2 should not have run on an empty database")
	}
}

// Once the database is up to date, applying again does nothing at all.
func TestRevisionsAreNotReplayed(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, revision_paths)
	exec(t, uri, "insert into test (name, age) values ('someone', 30);")
	mustApply(t, uri, revision_paths)

	if got := queryInt(t, uri, "select count(*) from test where name = 'someone'"); got != 1 {
		t.Error("re-applying an up-to-date database must not touch its data")
	}
}

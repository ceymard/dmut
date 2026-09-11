package main

import (
	"testing"

	"github.com/ceymard/dmut/v2/mutations"
)

// A small schema with no roles in it : roles are cluster-wide and would collide
// between tests sharing the same postgres.
const basic = `api:
  core:
    - create schema api;

api.users:
  core:
    - |
      create table api.users (
        id serial primary key,
        name text not null
      );
  glue:
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
  core:
    - create schema api;

api.users:
  core:
    - |
      create table api.users (
        id serial primary key,
        name text not null,
        email text
      );
  glue:
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
  core:
    - create schema api;

api.users:
  core:
    - |
      create table api.users (
        id serial primary key,
        name text not null
      );
  glue:
    - grant select, insert on table api.users to public;
`
	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": changed}))

	if got := queryInt(t, uri, "select count(*) from api.users"); got != 1 {
		t.Errorf("a meta change must not drop the table, found %d rows", got)
	}
}

// Appending a statement to core (e.g. a create index after a create table) must not
// down the table it applies to - this is the whole point of the longest-common-prefix
// diff (see specs/changes.md, "Why").
func TestAppendingCoreDoesNotDropTheTable(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basic}))
	exec(t, uri, "insert into api.users (name) values ('someone');")

	appended := `api:
  core:
    - create schema api;

api.users:
  core:
    - |
      create table api.users (
        id serial primary key,
        name text not null
      );
    - create index users_name_idx on api.users (name);
  glue:
    - grant select on table api.users to public;
`
	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": appended}))

	if got := queryInt(t, uri, "select count(*) from api.users"); got != 1 {
		t.Errorf("a pure append must not drop the table, found %d rows", got)
	}
	if !exists(t, uri, "select 1 from pg_indexes where indexname = 'users_name_idx'") {
		t.Error("the appended index should have been created")
	}
}

// simple mode must refuse a core change that isn't a pure append, and leave the
// database exactly as it was - no partial down, no partial up.
func TestSimpleModeRefusesNonAppendCoreChange(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basic}))
	exec(t, uri, "insert into api.users (name) values ('someone');")

	changed := `api:
  core:
    - create schema api;

api.users:
  core:
    - |
      create table api.users (
        id serial primary key,
        name text not null,
        email text
      );
  glue:
    - grant select on table api.users to public;
`
	err := ApplyCmd{Uri: uri, Paths: []string{mutationDir(t, map[string]string{"a.yml": changed})}, Simple: true}.Run()
	requireErrorContaining(t, err, "simple mode")

	if columnExists(t, uri, "users", "email") {
		t.Error("simple mode must not have touched the table at all")
	}
	if got := queryInt(t, uri, "select count(*) from api.users"); got != 1 {
		t.Errorf("simple mode must not have touched the data, found %d rows", got)
	}
}

// simple mode must still allow a pure append, exactly like default mode.
func TestSimpleModeAllowsPureAppend(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basic}))
	exec(t, uri, "insert into api.users (name) values ('someone');")

	appended := `api:
  core:
    - create schema api;

api.users:
  core:
    - |
      create table api.users (
        id serial primary key,
        name text not null
      );
    - create index users_name_idx on api.users (name);
  glue:
    - grant select on table api.users to public;
`
	err := ApplyCmd{Uri: uri, Paths: []string{mutationDir(t, map[string]string{"a.yml": appended})}, Simple: true}.Run()
	if err != nil {
		t.Fatalf("expected a pure append to succeed under simple mode, got: %v", err)
	}

	if got := queryInt(t, uri, "select count(*) from api.users"); got != 1 {
		t.Errorf("expected the row to survive, found %d rows", got)
	}
	if !exists(t, uri, "select 1 from pg_indexes where indexname = 'users_name_idx'") {
		t.Error("the appended index should have been created")
	}
}

// A schema with a table that has a dependent: another table whose foreign key
// references it. Changing the parent's core forces a tail-only down of just the
// parent, which Postgres refuses ("cannot drop table ... because other objects
// depend on it") since the dependent's foreign key is still there - the exact
// family of failure `full` mode's cascade-and-retry exists for.
const withDependent = `api:
  core:
    - create schema api;

api.accounts:
  core:
    - |
      create table api.accounts (
        id serial primary key,
        balance int
      );

api.transactions:
  needs: [api.accounts]
  core:
    - |
      create table api.transactions (
        id serial primary key,
        account_id int references api.accounts (id)
      );
`

const withDependentRetyped = `api:
  core:
    - create schema api;

api.accounts:
  core:
    - |
      create table api.accounts (
        id serial primary key,
        balance bigint
      );

api.transactions:
  needs: [api.accounts]
  core:
    - |
      create table api.transactions (
        id serial primary key,
        account_id int references api.accounts (id)
      );
`

// default mode must not cascade to a dependent on its own - it tries the tail-only
// down, Postgres refuses because of the dependent's foreign key, and the whole apply
// aborts with that error, leaving the database untouched.
func TestDefaultModeDoesNotCascadeOnFailure(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": withDependent}))
	exec(t, uri, "insert into api.accounts (balance) values (100);")

	err := apply(t, uri, mutationDir(t, map[string]string{"a.yml": withDependentRetyped}))
	requireErrorContaining(t, err, "depend")

	if !columnExists(t, uri, "accounts", "balance") {
		t.Error("the table should still be there at all")
	}
	if got := queryInt(t, uri, "select count(*) from api.accounts"); got != 1 {
		t.Errorf("default mode must not have touched the data, found %d rows", got)
	}
}

// full mode must fall back to rebuilding the dependent (and the mutation whose tail
// attempt failed) in full when the tail-only attempt is blocked, succeeding where
// default mode aborts.
func TestFullModeCascadesOnFailure(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": withDependent}))
	exec(t, uri, "insert into api.accounts (balance) values (100);")

	err := ApplyCmd{Uri: uri, Paths: []string{mutationDir(t, map[string]string{"a.yml": withDependentRetyped})}, Full: true}.Run()
	if err != nil {
		t.Fatalf("expected full mode to rebuild past the dependent, got: %v", err)
	}

	if !exists(t, uri, "select 1 from information_schema.columns where table_name = 'accounts' and column_name = 'balance' and data_type = 'bigint'") {
		t.Error("expected balance to have been retyped to bigint")
	}
	if !tableExists(t, uri, "transactions") {
		t.Error("expected the dependent table to have been rebuilt, not left dropped")
	}
	// A full rebuild tears the table down and recreates it, so the row inserted
	// before the apply does not survive - that's the actual trade-off `full` mode
	// makes, documented here rather than left implicit.
	if got := queryInt(t, uri, "select count(*) from api.accounts"); got != 0 {
		t.Errorf("expected the full rebuild to have lost the pre-existing row, found %d", got)
	}
}

// Removing a mutation downs it, and everything that depended on it.
func TestRemovingAMutationDownsIt(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basic}))
	mustApply(t, uri, mutationDir(t, map[string]string{
		"a.yml": "api:\n  core:\n    - create schema api;\n",
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
  core:
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
  core:
    - create schema good;

bad:
  needs: [good]
  core:
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
  core:
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

// DeleteMutations (the primitive a revision's delete: key uses) removes bookkeeping
// for specific mutations only - unlike ClearMutations (namespace-wide) - and, crucially,
// never touches the physical object it was tracking.
func TestDeleteMutationsRemovesOnlyNamedRows(t *testing.T) {
	uri := testdb(t)
	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": basic}))

	if got := savedMutations(t, uri, ""); got != 2 {
		t.Fatalf("expected 2 mutations recorded before delete, got %d", got)
	}

	runner, err := mutations.NewPgRunner(uri, false)
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Close()

	if err := runner.Begin(); err != nil {
		t.Fatal(err)
	}
	if err := runner.DeleteMutations("", []string{"api.users"}); err != nil {
		t.Fatal(err)
	}
	if err := runner.Commit(); err != nil {
		t.Fatal(err)
	}

	if got := savedMutations(t, uri, ""); got != 1 {
		t.Errorf("expected 1 mutation left after deleting api.users, got %d", got)
	}
	if !tableExists(t, uri, "users") {
		t.Error("DeleteMutations must not have touched the physical table, only bookkeeping")
	}
}

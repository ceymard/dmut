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

// A three-level dependency chain: transactions -> cards -> accounts, each with a
// real foreign key on the one before it. Retyping accounts.balance forces a
// tail-only down of accounts, blocked by cards' FK - and rebuilding cards in full is
// itself blocked by charges' FK on cards, so full mode's cascade has to actually
// recurse two levels deep, not stop at the immediate dependent.
const threeLevelChain = `api:
  core:
    - create schema api;

api.accounts:
  core:
    - |
      create table api.accounts (
        id serial primary key,
        balance int
      );

api.cards:
  needs: [api.accounts]
  core:
    - |
      create table api.cards (
        id serial primary key,
        account_id int references api.accounts (id)
      );

api.charges:
  needs: [api.cards]
  core:
    - |
      create table api.charges (
        id serial primary key,
        card_id int references api.cards (id)
      );
`

const threeLevelChainRetyped = `api:
  core:
    - create schema api;

api.accounts:
  core:
    - |
      create table api.accounts (
        id serial primary key,
        balance bigint
      );

api.cards:
  needs: [api.accounts]
  core:
    - |
      create table api.cards (
        id serial primary key,
        account_id int references api.accounts (id)
      );

api.charges:
  needs: [api.cards]
  core:
    - |
      create table api.charges (
        id serial primary key,
        card_id int references api.cards (id)
      );
`

// full mode must rebuild every level of the dependency chain, not just the direct
// dependent - dropping accounts is blocked by cards' FK, and dropping cards (to
// rebuild it) is in turn blocked by charges' FK on cards, so ExpandForFullRebuild's
// walk has to actually recurse.
func TestFullModeCascadesTwoLevelsDeep(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": threeLevelChain}))
	exec(t, uri, "insert into api.accounts (balance) values (100);")

	err := ApplyCmd{Uri: uri, Paths: []string{mutationDir(t, map[string]string{"a.yml": threeLevelChainRetyped})}, Full: true}.Run()
	if err != nil {
		t.Fatalf("expected full mode to rebuild both levels of dependents, got: %v", err)
	}

	if !exists(t, uri, "select 1 from information_schema.columns where table_name = 'accounts' and column_name = 'balance' and data_type = 'bigint'") {
		t.Error("expected balance to have been retyped to bigint")
	}
	if !tableExists(t, uri, "cards") {
		t.Error("expected the first-level dependent to have been rebuilt")
	}
	if !tableExists(t, uri, "charges") {
		t.Error("expected the second-level dependent (grandchild) to have been rebuilt too, not left orphaned")
	}
}

// api.accounts' target definition is simply broken SQL, so both the tail-only
// attempt *and* full mode's expanded rebuild fail to re-up it. There is no third
// attempt: the whole apply must abort and roll back exactly as cleanly as default
// mode does, leaving the database completely untouched by either attempt.
const withDependentBrokenRetype = `api:
  core:
    - create schema api;

api.accounts:
  core:
    - |
      create table api.accounts (
        id serial primary key,
        balance not_a_real_type
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

func TestFullModeAbortsWhenTheRebuildAlsoFails(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": withDependent}))
	exec(t, uri, "insert into api.accounts (balance) values (100);")

	err := ApplyCmd{Uri: uri, Paths: []string{mutationDir(t, map[string]string{"a.yml": withDependentBrokenRetype})}, Full: true}.Run()
	if err == nil {
		t.Fatal("expected the apply to fail - the rebuild's target definition is broken SQL")
	}

	if !exists(t, uri, "select 1 from information_schema.columns where table_name = 'accounts' and column_name = 'balance' and data_type = 'integer'") {
		t.Error("expected the original (int) column to survive a fully-aborted apply")
	}
	if got := queryInt(t, uri, "select count(*) from api.accounts"); got != 1 {
		t.Errorf("expected the pre-existing row to survive a clean rollback, found %d rows", got)
	}
}

// Two indexes; the first one is removed, shifting the second into its slot. simple
// mode must refuse this exactly like an edit at the tail would, even though the
// surviving index's own text never changed - it's the position that changed.
const withTwoIndexes = `api:
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
    - create index users_extra_idx on api.users ((name || '!'));
  glue:
    - grant select on table api.users to public;
`

const withMiddleIndexRemoved = `api:
  core:
    - create schema api;

api.users:
  core:
    - |
      create table api.users (
        id serial primary key,
        name text not null
      );
    - create index users_extra_idx on api.users ((name || '!'));
  glue:
    - grant select on table api.users to public;
`

func TestSimpleModeRefusesRemovalFromTheMiddle(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": withTwoIndexes}))
	exec(t, uri, "insert into api.users (name) values ('someone');")

	err := ApplyCmd{Uri: uri, Paths: []string{mutationDir(t, map[string]string{"a.yml": withMiddleIndexRemoved})}, Simple: true}.Run()
	requireErrorContaining(t, err, "simple mode")

	if !exists(t, uri, "select 1 from pg_indexes where indexname = 'users_name_idx'") {
		t.Error("simple mode must not have touched the table at all - users_name_idx should still be there")
	}
	if got := queryInt(t, uri, "select count(*) from api.users"); got != 1 {
		t.Errorf("simple mode must not have touched the data, found %d rows", got)
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

// A revision's delete: key untracks a mutation without running a down for it - the
// physical object is left completely alone, and a fresh database given only the
// revision that deletes it never tries to reach back and build what it names.
func TestRevisionDeleteDoesNotDownThePhysicalObject(t *testing.T) {
	uri := testdb(t)

	dir := mutationDir(t, map[string]string{
		"r1.yml": "__revision: 1\n" + basic,
		// api.users must still be restated in mutations: - each revision is
		// self-contained, not a diff against the one before, so omitting it here
		// would down it via the ordinary "absent locally" path, which is exactly
		// what this test needs to *not* happen. delete: is what untracks it while
		// leaving it fully present and unchanged.
		"r2.yml": `__revision: 2
delete: [api.users]

mutations:
  api:
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
`,
	})

	// Reach revision 1 for real first (a fresh apply otherwise skips straight to the
	// highest revision - not what this test is exercising).
	mustApply(t, uri, dir+"/r1.yml")
	mustApply(t, uri, dir)

	if !tableExists(t, uri, "users") {
		t.Error("delete: must not have downed the physical table")
	}
	if got := savedMutations(t, uri, ""); got != 1 {
		t.Errorf("expected only api left tracked after deleting api.users, got %d", got)
	}
}

const withDependentRetypedFullMode = `mode: full

mutations:
  api:
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

// A revision's own mode: wins outright over whatever the CLI invocation asked for -
// this is how a reviewed, committed revision carries destructive intent through an
// automated pipeline that otherwise always runs -s (specs/changes.md, "Apply changes").
func TestRevisionModeOverridesCliSimple(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": withDependent}))

	// Simple mode alone would refuse this outright (the same failure
	// TestDefaultModeDoesNotCascadeOnFailure exercises) - but the revision declares
	// mode: full itself, which must win over the CLI's -s.
	err := ApplyCmd{
		Uri:    uri,
		Paths:  []string{mutationDir(t, map[string]string{"a.yml": withDependentRetypedFullMode})},
		Simple: true,
	}.Run()
	if err != nil {
		t.Fatalf("expected the revision's own mode: full to override -s, got: %v", err)
	}

	if !exists(t, uri, "select 1 from information_schema.columns where table_name = 'accounts' and column_name = 'balance' and data_type = 'bigint'") {
		t.Error("expected balance to have been retyped to bigint")
	}
	if !tableExists(t, uri, "transactions") {
		t.Error("expected the dependent table to have been rebuilt, not left dropped")
	}
}

// override: may name a mutation that never appears in mutations: at all - per
// WithOverrides' own doc comment this "orphan" case is allowed and must still be
// recorded, so a later apply already knows about it. But it's never executed, and
// the second test pass assumes what it tests is already physically up (see
// TestMutationSet's own doc comment) - so an orphan override only actually works
// for adopting something that already exists (created out of band here, standing in
// for e.g. a table some other process manages), the same spirit as the --override
// CLI flag's TestOverrideAdoptsExistingObjects, just scoped to one mutation.
func TestOrphanOverrideAdoptsAnAlreadyExistingObject(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, mutationDir(t, map[string]string{"r1.yml": "__revision: 1\n" + basic}))

	exec(t, uri, "create table api.audit_log (id serial primary key, action text not null);")
	exec(t, uri, "insert into api.audit_log (action) values ('seeded out of band');")

	dir := mutationDir(t, map[string]string{
		"r2.yml": `__revision: 2

mutations:
  api:
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

override:
  api.audit_log:
    core:
      - create table api.audit_log (id serial primary key, action text not null);
`,
	})

	mustApply(t, uri, dir)

	if got := queryInt(t, uri, "select count(*) from api.audit_log"); got != 1 {
		t.Errorf("the orphan override must never execute - the pre-existing row must survive untouched, found %d rows", got)
	}
	if got := savedMutations(t, uri, ""); got != 3 {
		t.Errorf("expected api, api.users and the orphan api.audit_log all recorded, got %d", got)
	}
}

// The other side of the same coin: an orphan override for something that was never
// physically created at all (no out-of-band adoption, nothing in mutations: either)
// cannot pass the second test pass - there's nothing there for it to verify
// reversibility against - and must fail loudly rather than silently recording
// bookkeeping for an object that doesn't exist.
func TestOrphanOverrideForNonExistentObjectFailsLoudly(t *testing.T) {
	uri := testdb(t)

	dir := mutationDir(t, map[string]string{
		"r2.yml": `__revision: 2

mutations:
  api:
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

override:
  api.audit_log:
    core:
      - create table api.audit_log (id serial primary key, action text not null);
`,
	})

	err := apply(t, uri, dir)
	requireErrorContaining(t, err, "does not exist")

	if got := savedMutations(t, uri, ""); got != 0 {
		t.Errorf("a failed apply must not have recorded anything, got %d rows", got)
	}
}

// When the same name is named in both delete: and override:, delete: must win (per
// WithOverrides' doc comment) - the mutation disappears from tracking entirely,
// despite an override being provided for it, and the physical object is never
// touched either way.
func TestDeleteWinsOverOverrideForTheSameName(t *testing.T) {
	uri := testdb(t)

	dir := mutationDir(t, map[string]string{
		"r1.yml": "__revision: 1\n" + basic,
		"r2.yml": `__revision: 2

delete: [api.users]

mutations:
  api:
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

override:
  api.users:
    core:
      - create table api.users (id serial primary key, name text not null, extra text);
`,
	})

	mustApply(t, uri, dir+"/r1.yml")
	mustApply(t, uri, dir)

	if !tableExists(t, uri, "users") {
		t.Error("the physical table must still be there - delete: never downs anything")
	}
	if got := savedMutations(t, uri, ""); got != 1 {
		t.Errorf("expected only api left tracked - delete: must win over the override for the same name, got %d", got)
	}
}

// The mirror of TestRevisionModeOverridesCliSimple: a revision can also *tighten*
// safety, not just loosen it. mode: simple refuses a retype that CLI -f would
// otherwise happily cascade-rebuild through.
const withDependentRetypedSimpleMode = `mode: simple

mutations:
  api:
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

func TestRevisionModeSimpleOverridesCliFull(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, mutationDir(t, map[string]string{"a.yml": withDependent}))
	exec(t, uri, "insert into api.accounts (balance) values (100);")

	err := ApplyCmd{
		Uri:   uri,
		Paths: []string{mutationDir(t, map[string]string{"a.yml": withDependentRetypedSimpleMode})},
		Full:  true,
	}.Run()
	requireErrorContaining(t, err, "simple mode")

	if !exists(t, uri, "select 1 from information_schema.columns where table_name = 'accounts' and column_name = 'balance' and data_type = 'integer'") {
		t.Error("mode: simple must have refused before touching anything - balance should still be int")
	}
	if got := queryInt(t, uri, "select count(*) from api.accounts"); got != 1 {
		t.Errorf("expected the pre-existing row to survive, found %d rows", got)
	}
}

// A broken override (its down is a lie, exactly like TestAMutationThatCannotBeDownedIsRefused)
// on an already-adopted object is caught by RunMutations' second test pass
// ("performing tests with override"), which actually exercises the override-swapped
// set - not just the first pass against what really ran. Without that second pass, a
// bad override would be recorded silently and only bite whenever some future
// revision inherited it.
func TestBrokenOverrideIsCaughtBySecondTestPass(t *testing.T) {
	uri := testdb(t)

	mustApply(t, uri, mutationDir(t, map[string]string{"r1.yml": "__revision: 1\n" + basic}))
	if got := savedMutations(t, uri, ""); got != 2 {
		t.Fatalf("expected 2 mutations recorded after r1, got %d", got)
	}

	// Adopted from elsewhere, same as TestOrphanOverrideAdoptsAnAlreadyExistingObject -
	// otherwise the second pass would fail on "does not exist" before the lying down
	// statement ever gets a chance to matter.
	exec(t, uri, "create table api.lying (id int);")

	dir := mutationDir(t, map[string]string{
		"r2.yml": `__revision: 2

mutations:
  api:
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

override:
  api.lying:
    core:
      - up: create table api.lying (id int);
        down: select 1;
`,
	})

	err := apply(t, uri, dir)
	requireErrorContaining(t, err, "already exists")

	if got := savedMutations(t, uri, ""); got != 2 {
		t.Errorf("a failed apply must not have recorded the broken override, got %d rows", got)
	}
}

// A bare rename (parent.old_name -> parent.new_name, same effect, no delete:/
// override:) is not a squash - it's the natural first mistake someone makes reaching
// for "consolidate this," so it's worth pinning down exactly what actually happens:
// the old name goes down in full and the new name comes up as brand new, just like
// any other obsolete+new pair, with no special-casing for the rename "intent".
func TestRenamingAChildMutationIsNotASquash(t *testing.T) {
	uri := testdb(t)

	dir1 := mutationDir(t, map[string]string{"a.yml": `api:
  core:
    - create schema api;

api.users:
  core:
    - create table api.users (id serial primary key, name text not null);

api.users.legacy_flag:
  needs: [api.users]
  core:
    - alter table api.users add column legacy_flag boolean not null default false;
`})
	mustApply(t, uri, dir1)
	exec(t, uri, "insert into api.users (name) values ('someone');")

	dir2 := mutationDir(t, map[string]string{"a.yml": `api:
  core:
    - create schema api;

api.users:
  core:
    - create table api.users (id serial primary key, name text not null);

api.users.retired_flag:
  needs: [api.users]
  core:
    - alter table api.users add column retired_flag boolean not null default false;
`})
	mustApply(t, uri, dir2)

	if columnExists(t, uri, "users", "legacy_flag") {
		t.Error("the old name should have gone down in full, like any other obsolete mutation")
	}
	if !columnExists(t, uri, "users", "retired_flag") {
		t.Error("the new name should have come up as brand new")
	}
	if got := queryInt(t, uri, "select count(*) from api.users"); got != 1 {
		t.Errorf("api.users itself was untouched, so the row should survive the rename of its sibling, found %d rows", got)
	}
	if got := savedMutations(t, uri, ""); got != 3 {
		t.Errorf("expected api, api.users and api.users.retired_flag tracked (legacy_flag gone), got %d", got)
	}
}

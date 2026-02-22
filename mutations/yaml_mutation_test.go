package mutations

import (
	"path/filepath"
	"runtime"
	"testing"
)

func testFixturePath() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "test", "test.yml")
}

func TestReadYamlFile_ReturnsAllDocuments(t *testing.T) {
	muts, err := readYamlFile(testFixturePath())
	if err != nil {
		t.Fatalf("readYamlFile returned error: %v", err)
	}
	if len(muts) != 5 {
		t.Fatalf("expected 5 documents, got %d", len(muts))
	}
}

func TestReadYamlFile_Names(t *testing.T) {
	muts, err := readYamlFile(testFixturePath())
	if err != nil {
		t.Fatalf("readYamlFile returned error: %v", err)
	}

	expected := []string{"pgcrypto", "auth", "auth.passwords", "api", "api.users"}
	for i, want := range expected {
		if muts[i].Name != want {
			t.Errorf("document %d: expected name %q, got %q", i, want, muts[i].Name)
		}
	}
}

func TestReadYamlFile_Roles(t *testing.T) {
	muts, err := readYamlFile(testFixturePath())
	if err != nil {
		t.Fatalf("readYamlFile returned error: %v", err)
	}

	// Document 1 (auth) has roles
	authMut := muts[1]
	wantRoles := []string{"@admin", "@active"}
	if len(authMut.Roles) != len(wantRoles) {
		t.Fatalf("auth: expected %d roles, got %d", len(wantRoles), len(authMut.Roles))
	}
	for i, want := range wantRoles {
		if authMut.Roles[i] != want {
			t.Errorf("auth role %d: expected %q, got %q", i, want, authMut.Roles[i])
		}
	}

	// Other documents should have no roles
	for i, mut := range muts {
		if i == 1 {
			continue
		}
		if len(mut.Roles) != 0 {
			t.Errorf("document %d (%s): expected no roles, got %v", i, mut.Name, mut.Roles)
		}
	}
}

func TestReadYamlFile_SqlStatements(t *testing.T) {
	muts, err := readYamlFile(testFixturePath())
	if err != nil {
		t.Fatalf("readYamlFile returned error: %v", err)
	}

	tests := []struct {
		docIndex int
		name     string
		wantLen  int
		firstUp  string
	}{
		{0, "pgcrypto", 1, "create extension pgcrypto;"},
		{1, "auth", 1, "create schema auth;"},
		{3, "api", 1, "create schema api;\n"},
	}

	for _, tc := range tests {
		mut := muts[tc.docIndex]
		if len(mut.Sql) != tc.wantLen {
			t.Errorf("%s: expected %d sql statements, got %d", tc.name, tc.wantLen, len(mut.Sql))
			continue
		}
		if mut.Sql[0].Up != tc.firstUp {
			t.Errorf("%s: expected Up=%q, got %q", tc.name, tc.firstUp, mut.Sql[0].Up)
		}
	}
}

func TestReadYamlFile_SqlAutoDown(t *testing.T) {
	muts, err := readYamlFile(testFixturePath())
	if err != nil {
		t.Fatalf("readYamlFile returned error: %v", err)
	}

	tests := []struct {
		docIndex int
		name     string
		wantDown string
	}{
		{0, "pgcrypto", "DROP EXTENSION pgcrypto;"},
		{1, "auth", "DROP SCHEMA auth;"},
	}

	for _, tc := range tests {
		mut := muts[tc.docIndex]
		if len(mut.Sql) == 0 {
			t.Errorf("%s: no sql statements", tc.name)
			continue
		}
		if mut.Sql[0].Down != tc.wantDown {
			t.Errorf("%s: expected Down=%q, got %q", tc.name, tc.wantDown, mut.Sql[0].Down)
		}
	}
}

func TestReadYamlFile_ExplicitUpDown(t *testing.T) {
	muts, err := readYamlFile(testFixturePath())
	if err != nil {
		t.Fatalf("readYamlFile returned error: %v", err)
	}

	// Document 4 (api.users) has a multi-line CREATE TABLE
	usersMut := muts[4]
	if len(usersMut.Sql) == 0 {
		t.Fatal("api.users: expected sql statements")
	}
	if usersMut.Sql[0].Up == "" {
		t.Error("api.users: expected non-empty Up statement")
	}
}

func TestReadYamlFile_NodeIsPopulated(t *testing.T) {
	muts, err := readYamlFile(testFixturePath())
	if err != nil {
		t.Fatalf("readYamlFile returned error: %v", err)
	}

	for i, mut := range muts {
		if mut.Node == nil {
			t.Errorf("document %d (%s): Node should be populated", i, mut.Name)
		}
	}
}

func TestLoadYamlMutations_Directory(t *testing.T) {
	dir := filepath.Dir(testFixturePath())
	muts, err := LoadYamlMutations(dir)
	if err != nil {
		t.Fatalf("LoadYamlMutations returned error: %v", err)
	}
	if len(muts) != 5 {
		t.Fatalf("expected 5 mutations from directory, got %d", len(muts))
	}
}

func TestLoadYamlMutations_File(t *testing.T) {
	muts, err := LoadYamlMutations(testFixturePath())
	if err != nil {
		t.Fatalf("LoadYamlMutations returned error: %v", err)
	}
	if len(muts) != 5 {
		t.Fatalf("expected 5 mutations from file, got %d", len(muts))
	}
}

func TestLoadYamlMutations_SkipsUnderscoreFiles(t *testing.T) {
	dir := filepath.Dir(testFixturePath())

	// LoadYamlMutations should skip files prefixed with _
	muts, err := LoadYamlMutations(dir)
	if err != nil {
		t.Fatalf("LoadYamlMutations returned error: %v", err)
	}
	// All results come from test.yml only (no _-prefixed files)
	if len(muts) != 5 {
		t.Errorf("expected 5 mutations, got %d", len(muts))
	}
}

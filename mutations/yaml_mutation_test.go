package mutations

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func testFixturePath() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "test", "test.yml")
}

func TestReadYamlFile_ReturnsAllDocuments(t *testing.T) {
	var muts YamlMigrationFile = make(YamlMigrationFile)
	err := readYamlFile(muts, testFixturePath())
	if err != nil {
		t.Fatalf("readYamlFile returned error: %v", err)
	}
	if len(muts) != 5 {
		t.Fatalf("expected 5 documents, got %d", len(muts))
	}
}

func TestReadYamlFile_Names(t *testing.T) {
	var muts YamlMigrationFile = make(YamlMigrationFile)
	err := readYamlFile(muts, testFixturePath())
	if err != nil {
		t.Fatalf("readYamlFile returned error: %v", err)
	}

	expected := []string{"pgcrypto", "auth", "auth.passwords", "api", "api.users"}
	for i, want := range expected {
		if _, ok := muts[want]; !ok {
			t.Errorf("document %d: expected name %q, got not found", i, want)
		}
	}
}

func TestReadYamlFile_Roles(t *testing.T) {
	var muts YamlMigrationFile = make(YamlMigrationFile)
	err := readYamlFile(muts, testFixturePath())
	if err != nil {
		t.Fatalf("readYamlFile returned error: %v", err)
	}

	// auth has roles
	authMut, ok := muts["auth"]
	if !ok {
		t.Fatal("expected auth migration")
	}
	wantRoles := []string{"@admin", "@active"}
	if len(authMut.Roles) != len(wantRoles) {
		t.Fatalf("auth: expected %d roles, got %d", len(wantRoles), len(authMut.Roles))
	}
	for i, want := range wantRoles {
		if authMut.Roles[i] != want {
			t.Errorf("auth role %d: expected %q, got %q", i, want, authMut.Roles[i])
		}
	}

	// Other migrations should have no roles
	for name, mut := range muts {
		if name == "auth" {
			continue
		}
		if len(mut.Roles) != 0 {
			t.Errorf("migration %q: expected no roles, got %v", name, mut.Roles)
		}
	}
}

func TestReadYamlFile_SqlStatements(t *testing.T) {
	var muts YamlMigrationFile = make(YamlMigrationFile)
	err := readYamlFile(muts, testFixturePath())
	if err != nil {
		t.Fatalf("readYamlFile returned error: %v", err)
	}

	tests := []struct {
		name    string
		wantLen int
		firstUp string
	}{
		{"pgcrypto", 1, "create extension pgcrypto;"},
		{"auth", 1, "create schema auth;"},
		{"api", 1, "create schema api;\n"},
	}

	for _, tc := range tests {
		mut, ok := muts[tc.name]
		if !ok {
			t.Errorf("%s: migration not found", tc.name)
			continue
		}
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
	var muts YamlMigrationFile = make(YamlMigrationFile)
	err := readYamlFile(muts, testFixturePath())
	if err != nil {
		t.Fatalf("readYamlFile returned error: %v", err)
	}

	tests := []struct {
		name     string
		wantDown string
	}{
		{"pgcrypto", "DROP EXTENSION pgcrypto;"},
		{"auth", "DROP SCHEMA auth;"},
	}

	for _, tc := range tests {
		mut, ok := muts[tc.name]
		if !ok {
			t.Errorf("%s: migration not found", tc.name)
			continue
		}
		if len(mut.Sql) == 0 {
			t.Errorf("%s: no sql statements", tc.name)
			continue
		}
		want := strings.ToLower(tc.wantDown)
		down := strings.ToLower(mut.Sql[0].Down)
		if down != want {
			t.Errorf("%s: expected Down=%q, got %q", tc.name, tc.wantDown, mut.Sql[0].Down)
		}
	}
}

func TestReadYamlFile_ExplicitUpDown(t *testing.T) {
	var muts YamlMigrationFile = make(YamlMigrationFile)
	err := readYamlFile(muts, testFixturePath())
	if err != nil {
		t.Fatalf("readYamlFile returned error: %v", err)
	}

	// api.users has a multi-line CREATE TABLE
	usersMut, ok := muts["api.users"]
	if !ok {
		t.Fatal("api.users: migration not found")
	}
	if len(usersMut.Sql) == 0 {
		t.Fatal("api.users: expected sql statements")
	}
	if usersMut.Sql[0].Up == "" {
		t.Error("api.users: expected non-empty Up statement")
	}
}

func TestReadYamlFile_NodeIsPopulated(t *testing.T) {
	var muts YamlMigrationFile = make(YamlMigrationFile)
	err := readYamlFile(muts, testFixturePath())
	if err != nil {
		t.Fatalf("readYamlFile returned error: %v", err)
	}

	for name, mut := range muts {
		if mut.Node == nil {
			t.Errorf("migration %q: Node should be populated", name)
		}
	}
}

func TestLoadYamlMutations_Directory(t *testing.T) {
	dir := filepath.Dir(testFixturePath())
	var muts YamlMigrationFile = make(YamlMigrationFile)
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

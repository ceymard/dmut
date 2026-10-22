package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A mutation whose second statement blocks long enough for the context to expire
// after the first one has run inside the transaction.
const slowMutation = `slow:
  core:
    - create schema slow;
    - up: select pg_sleep(60);
      down: select 1;
`

// A context that is already cancelled stops the run before it touches anything.
func TestApplyWithCancelledContextFails(t *testing.T) {
	uri := testdb(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := ApplyCmd{Uri: uri, Paths: []string{mutationDir(t, map[string]string{"a.yml": basic})}}.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected an error wrapping context.Canceled, got: %v", err)
	}
	if schemaExists(t, uri, "api") {
		t.Error("nothing should have been applied")
	}
}

// A deadline hitting in the middle of a run aborts the statement in flight and
// leaves no trace: no object, no bookkeeping, no backend still working.
func TestApplyDeadlineMidRunLeavesNothingBehind(t *testing.T) {
	uri := testdb(t)
	dir := mutationDir(t, map[string]string{"a.yml": slowMutation})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	start := time.Now()
	err := ApplyCmd{Uri: uri, Paths: []string{dir}}.Run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected an error wrapping context.DeadlineExceeded, got: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 20*time.Second {
		t.Errorf("the run should stop at the deadline, it took %s", elapsed)
	}

	if schemaExists(t, uri, "slow") {
		t.Error("the slow schema should have been rolled back")
	}
	if savedMutations(t, uri, "") != 0 {
		t.Error("no mutation should have been recorded")
	}

	// The server may take a moment to notice the cancelled connection.
	deadline := time.Now().Add(5 * time.Second)
	for {
		busy := queryInt(t, uri, `select count(*) from pg_stat_activity
			where datname = current_database() and pid <> pg_backend_pid()
			and (query like '%pg_sleep%' or state like 'idle in transaction%')`)
		if busy == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d backend(s) still busy or holding a transaction after the run returned", busy)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

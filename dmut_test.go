package main

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var pgContainer *postgres.PostgresContainer
var pgURI string

func TestMain(m *testing.M) {
	ctx := context.Background()

	log.Printf("starting postgres container")
	// Start singleton Postgres container
	container, err := postgres.Run(ctx,
		"postgres:14",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
	)
	if err != nil {
		log.Fatalf("failed to start postgres: %v", err)
	}
	pgContainer = container

	// Get connection URI
	pgURI, err = pgContainer.ConnectionString(ctx)
	if err != nil {
		log.Fatalf("failed to get connection string: %v", err)
	}

	log.Printf("pgURI: %s", pgURI)

	// Run tests
	code := m.Run()

	// Shutdown container
	log.Printf("terminating container")
	if err := pgContainer.Terminate(ctx); err != nil {
		log.Printf("failed to terminate container: %v", err)
	}

	os.Exit(code)
}

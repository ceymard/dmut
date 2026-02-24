package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ceymard/dmut/mutations"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

type TestCmd struct {
	Image   string   `short:"i" help:"Postgres image name to test on."`
	Verbose bool     `short:"v" help:"Verbose output."`
	Paths   []string `arg:"" help:"Paths to test."`
}

type Printer struct{}

func (Printer) Accept(l testcontainers.Log) {
	fmt.Print(string(l.Content))
}

func (t TestCmd) Run() error {
	var image = "postgres:14"
	if t.Image != "" {
		image = t.Image
	}
	log.Println("testing mutations on", image)

	muts, err := mutations.LoadYamlMutations(t.Paths...)
	if err != nil {
		return err
	}

	ctx := context.Background()
	container, err := postgres.Run(ctx,
		image,
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(5*time.Second),
		),
	)
	if err != nil {
		return err
	}
	defer container.Terminate(ctx)

	if t.Verbose {
		printer := Printer{}
		container.FollowOutput(printer)
		container.StartLogProducer(ctx)
	}

	log.Println("container started, waiting for it to be ready")

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		return err
	}
	log.Println("test container URI:", uri)

	runner, err := mutations.NewPgRunner(uri, false)
	if err != nil {
		return err
	}
	defer runner.Close()

	db_mutations := muts.ToDbMutationMap()
	roles := muts.Roles()

	if err := mutations.ApplyMutations(runner, db_mutations, roles); err != nil {
		return err
	}

	return nil
}

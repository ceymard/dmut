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
	Verbose  bool     `short:"v" help:"Verbose output."`
	Image    string   `short:"i" name:"test-image" help:"Postgres image name to test on."`
	Database string   `short:"d" name:"test-database" help:"Database name to test on."`
	Username string   `short:"u" name:"test-username" help:"Username to test on."`
	Password string   `short:"p" name:"test-password" help:"Password to test on."`
	Paths    []string `arg:"" help:"Paths to test."`
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
	if t.Database == "" {
		t.Database = "test"
	}
	if t.Username == "" {
		t.Username = "test"
	}
	if t.Password == "" {
		t.Password = "test"
	}
	log.Println("testing mutations on", image)

	muts, err := mutations.LoadYamlMutations(t.Paths...)
	if err != nil {
		return err
	}

	ctx := context.Background()
	container, err := postgres.Run(ctx,
		image,
		postgres.WithDatabase(t.Database),
		postgres.WithUsername(t.Username),
		postgres.WithPassword(t.Password),
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

	runner, err := mutations.NewPgRunner(uri, t.Verbose)
	if err != nil {
		return err
	}
	defer runner.Close()

	db_mutations := muts.ToDbMutationMap()
	roles := muts.Roles()

	if err := mutations.RunMutations(runner, db_mutations, roles, &mutations.MutationRunnerOptions{
		TestBefore: true,
	}); err != nil {
		return err
	}

	return nil
}

package main

// Overwrite the destination database with the definition provided in the yaml paths

type OverwriteCmd struct {
	Host  string   `arg:"" help:"Database host."`
	Paths []string `arg:"" help:"Paths to the mutation files"`
}

func (o OverwriteCmd) Run() error {
	return nil
}

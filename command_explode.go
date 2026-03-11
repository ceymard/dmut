package main

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/k0kubun/pp"
)

type ExplodeCmd struct {
	OutDir string   `required:"" short:"o" name:"out-dir" help:"Output directory to write the mutations to."`
	Paths  []string `arg:"" help:"Paths to the mutation files"`
}

func (e ExplodeCmd) Run() error {

	re_mutation := regexp.MustCompile(`(?m)^([^:\n]+):[ \t]*\n(([ \t][^\n]*)?\n)*`)

	type naive_mutation struct {
		Name    string
		Content string
	}

	for _, pth := range e.Paths {
		bytes, err := os.ReadFile(pth)
		if err != nil {
			return err
		}
		matches := re_mutation.FindAllStringSubmatch(string(bytes), -1)
		pp.Println(matches)
		for _, match := range matches {
			name := match[1]
			contents := match[0]

			components := strings.Split(name, ".")
			last_name := components[len(components)-1]
			out_file := path.Join(e.OutDir, last_name+".yml")
			if len(components) > 1 {
				dir := path.Join(components[:len(components)-1]...)
				if err := os.MkdirAll(filepath.Join(e.OutDir, dir), 0755); err != nil {
					return err
				}
				out_file = path.Join(e.OutDir, dir, last_name+".yml")
			}

			if err := os.WriteFile(out_file, []byte(contents), 0644); err != nil {
				return err
			}
			fmt.Println(out_file)

		}
	}

	return nil
}

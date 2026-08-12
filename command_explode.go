package main

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/samber/oops"
)

type ExplodeCmd struct {
	OutDir string   `required:"" short:"o" name:"out-dir" help:"Output directory to write the mutations to."`
	Paths  []string `arg:"" help:"Paths to the mutation files"`
}

func (e ExplodeCmd) Run() error {

	re_mutation := regexp.MustCompile(`(?m)^([^:\n]+):[ \t]*\n(([ \t][^\n]*)?\n)*`)
	re_namespace := regexp.MustCompile(`(?m)^__namespace:[ \t]*(.+?)[ \t]*$`)

	type naive_mutation struct {
		Name    string
		Content string
	}

	for _, pth := range e.Paths {
		bytes, err := os.ReadFile(pth)
		if err != nil {
			return err
		}

		// Every exploded file is read back as a set of its own, so each one needs the
		// namespace. __revision is left out on purpose : unrevisioned files are the
		// current revision by definition.
		var prefix string
		if namespaces := re_namespace.FindAllStringSubmatch(string(bytes), -1); len(namespaces) > 0 {
			for _, match := range namespaces {
				if match[1] != namespaces[0][1] {
					return oops.In("explode").With("file", pth).Errorf(
						"%s declares several namespaces (%s and %s) : explode works on one revision at a time",
						pth, namespaces[0][1], match[1])
				}
			}
			prefix = "__namespace: " + namespaces[0][1] + "\n\n"
		}

		matches := re_mutation.FindAllStringSubmatch(string(bytes), -1)
		for _, match := range matches {
			name := match[1]
			contents := prefix + match[0]

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

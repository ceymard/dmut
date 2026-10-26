package main

import (
	"context"
	"os"
	"strings"

	"github.com/ceymard/dmut/v3/mutations/model"
	"github.com/goccy/go-yaml"
	"github.com/jackc/pgx/v5"
)

// Legacy commands that are no longer supported

type LegacyCmd struct {
	Host string `arg:"" help:"Database host."`
}

func (c LegacyCmd) Run(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, c.Host)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, "SELECT hash, name, up, down, children FROM dmut.mutations")
	if err != nil {
		return err
	}

	type LegacyMutation struct {
		Hash     string `db:"hash"`
		Needs    []string
		Name     string   `db:"name"`
		Up       []string `db:"up"`
		Down     []string `db:"down"`
		Children []string `db:"children"`
	}

	var mut_map = make(map[string]*LegacyMutation)

	for rows.Next() {
		var mut LegacyMutation = LegacyMutation{}

		err = rows.Scan(&mut.Hash, &mut.Name, &mut.Up, &mut.Down, &mut.Children)
		if err != nil {
			return err
		}

		if mut.Name != "dmut.base" && mut.Name != "dmut" {
			mut_map[mut.Name] = &mut
		}
	}

	for _, mut := range mut_map {
		for _, child := range mut.Children {
			child_mut := mut_map[child]
			child_mut.Needs = append(child_mut.Needs, mut.Name)
		}
	}

	type YamlStatement struct {
		Up   string `yaml:"up,omitempty"`
		Down string `yaml:"down,omitempty"`
	}
	type YamlMutation struct {
		Structural bool     `yaml:"structural"`
		Deps       []string `yaml:"deps,omitempty,flow"`
		Sql        []any    `yaml:"sql"`
	}
	type YamlFile struct {
		Revision  int                      `yaml:"revision"`
		Mutations map[string]*YamlMutation `yaml:"mutations"`
	}

	// Everything the old system did is arbitrary SQL: the mutations are structural, to be
	// narrowed by hand. A statement with no matching up or down is written with only the
	// half it has, so that loading the file fails there and it gets written by hand.
	var yaml_file = YamlFile{Revision: 1, Mutations: make(map[string]*YamlMutation)}
	for _, mut := range mut_map {
		var statements = make([]any, 0)
		if len(mut.Up) != len(mut.Down) {

			for _, up := range mut.Up {
				statements = append(statements, YamlStatement{Up: replaceLineComments(up)})
			}

			// add down in reverse order
			for i := len(mut.Down) - 1; i >= 0; i-- {
				statements = append(statements, YamlStatement{Down: replaceLineComments(mut.Down[i])})
			}
		} else {
			for i, up := range mut.Up {

				up = replaceLineComments(up)
				down := replaceLineComments(mut.Down[len(mut.Down)-i-1])

				if inferred, err := model.Infer(up); err == nil && inferred.Down == down {
					// if we can undo it as it was, just put the string
					statements = append(statements, up)
				} else {
					statements = append(statements, YamlStatement{
						Up:   up,
						Down: down,
					})
				}
			}
		}

		yaml_file.Mutations[mut.Name] = &YamlMutation{
			Structural: true,
			Deps:       mut.Needs,
			Sql:        statements,
		}
	}

	yaml.NewEncoder(os.Stdout).Encode(yaml_file)
	// yaml_file := ast.
	// yaml.NewEncoder()
	// yaml_file.Kind = yaml.MappingNode
	// yaml_file.Tag = "!!map"
	// yaml_file.Content = make([]*yaml.Node, 0)

	// for _, mut := range mut_map {
	// 	mut_node := yaml.Node{}
	// 	mut_node.Kind = yaml.MappingNode
	// 	mut_node.Tag = "!!map"
	// 	mut_node.Content = make([]*yaml.Node, 0)
	// }
	return nil
}

// Replace line comments that start with --, keep the comment but put it between /* and */
func replaceLineComments(str string) string {
	lines := strings.Split(str, "\n")
	for i, line := range lines {
		if idx := strings.Index(line, "--"); idx >= 0 {
			comment := line[idx+2:]
			lines[i] = line[:idx] + "/* " + comment + " */"
		}
	}
	return strings.Join(lines, "\n")
}

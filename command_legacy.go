package main

import (
	"context"
	"os"
	"strings"

	"github.com/ceymard/dmut/mutations"
	"github.com/goccy/go-yaml"
	"github.com/jackc/pgx/v4"
)

// Legacy commands that are no longer supported

type LegacyCmd struct {
	Host string `arg:"" help:"Database host."`
}

func (c LegacyCmd) Run() error {
	conn, err := pgx.Connect(context.Background(), c.Host)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(context.Background(), "SELECT hash, name, up, down, children FROM dmut.mutations")
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

	type YamlLegacyStatementUpDown struct {
		Up   string `yaml:"up"`
		Down string `yaml:"down"`
	}
	type YamlLegacyStatement interface{}
	type YamlLegacyMigration struct {
		Needs []string `yaml:"needs,omitempty,flow"`
		Sql   []YamlLegacyStatement
	}
	type YamlFile map[string]*YamlLegacyMigration

	var yaml_file = make(YamlFile)
	for _, mut := range mut_map {
		var statements = make([]YamlLegacyStatement, 0)
		if len(mut.Up) != len(mut.Down) {

			for _, up := range mut.Up {
				statements = append(statements, mutations.YamlStatement{
					Up: replaceLineComments(up),
					// Down: mut.Down[len(mut.Down)-i-1],
				})
			}
			for _, down := range mut.Down {
				statements = append(statements, mutations.YamlStatement{
					Down: replaceLineComments(down),
				})
			}
		} else {
			for i, up := range mut.Up {

				up = replaceLineComments(up)
				down := replaceLineComments(mut.Down[len(mut.Down)-i-1])

				if _, err := mutations.Parser.ParseString("", up); err == nil {
					// if we can undo it, just put the string
					// os.Stdout.WriteString(up + "\n")
					statements = append(statements, up)
				} else {
					statements = append(statements, mutations.YamlStatement{
						Up:   up,
						Down: down,
					})
				}
			}
		}

		yaml_file[mut.Name] = &YamlLegacyMigration{
			Needs: mut.Needs,
			Sql:   statements,
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

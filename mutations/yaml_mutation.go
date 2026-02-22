package mutations

import (
	"fmt"
	"io"
	"os"

	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
)

type YamlMigration struct {
	Node    ast.Node
	Name    string          `yaml:"name"`
	Depends []string        `yaml:"depends"`
	Sql     []YamlStatement `yaml:"sql"`
	Perms   []YamlStatement `yaml:"perms"`
	Roles   []string        `yaml:"roles"`
}

func (m *YamlMigration) UnmarshalYAML(node ast.Node) error {
	m.Node = node

	// Check if there was data for the Sql key, but it was just a string instead of an array
	if node.Type() == ast.StringType {
		str_node, ok := node.(*ast.StringNode)
		if !ok {
			return fmt.Errorf("expected string node, got %T", node)
		}
		stmt, err := yamlStatementFromString(str_node.Value)
		if err != nil {
			return fmt.Errorf("error parsing yaml statement: %w", err)
		}
		m.Sql = []YamlStatement{stmt}
	}

	// var simpleSql string

	// dec := yaml.NewDecoder(node)
	// yaml.NodeToValue(node, &m.Sql)

	return nil
}

type YamlStatement struct {
	Node ast.Node
	Up   string `yaml:"up"`
	Down string `yaml:"down"`
}

func (stm *YamlStatement) UnmarshalYAML(node ast.Node) error {
	stm.Node = node

	// Check if the current node is a string instead of a mapping with Up and Down
	if node.Type() == ast.StringType {
		str_node, ok := node.(*ast.StringNode)
		if !ok {
			return fmt.Errorf("expected string node, got %T", node)
		}
		stmt, err := yamlStatementFromString(str_node.Value)
		if err != nil {
			return fmt.Errorf("error parsing yaml statement: %w", err)
		}
		stm.Down = stmt.Down
	}

	return nil
}

func yamlStatementFromString(str string) (YamlStatement, error) {
	res, err := Parser.ParseString("", str)
	if err != nil {
		return YamlStatement{}, fmt.Errorf("can't generate undo statement from %s: %w", str, err)
	}
	return YamlStatement{Up: str, Down: (*res).Down()}, nil
}

func readYamlFile(filename string) ([]YamlMigration, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error reading file %s: %w", filename, err)
	}
	defer f.Close()

	var res []YamlMigration
	dec := yaml.NewDecoder(f)
	for {
		var ym YamlMigration
		err = dec.Decode(&ym)
		if err == io.EOF {
			break // normal end of stream
		}

		if err != nil {
			return nil, fmt.Errorf("error decoding yaml: %w", err)
		}
		res = append(res, ym)
	}
	return res, nil
}

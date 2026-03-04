package mutations

import (
	"fmt"

	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
)

type MutationStatement struct {
	Node ast.Node `yaml:"-"`
	Up   string   `yaml:"up"`
	Down string   `yaml:"down"`
}

func (stm *MutationStatement) UnmarshalYAML(node ast.Node) error {
	stm.Node = node

	if node.Type() == ast.StringType || node.Type() == ast.LiteralType {
		var val string
		yaml.NodeToValue(node, &val)
		stmt, err := mutationStatementFromString(val)
		if err != nil {
			// pp.Println(node.GetToken().Position)
			return fmt.Errorf("error parsing yaml statement: %w", err)
		}
		stm.Up = stmt.Up
		stm.Down = stmt.Down
		return nil
	} else if node.Type() == ast.MappingType {
		map_node, ok := node.(*ast.MappingNode)
		if !ok {
			return fmt.Errorf("expected mapping node, got %T", node)
		}
		for _, value := range map_node.Values {
			if !ok {
				continue
			}
			if value.Key.String() == "up" {
				yaml.NodeToValue(value.Value, &stm.Up)
			} else if value.Key.String() == "down" {
				yaml.NodeToValue(value.Value, &stm.Down)
			}
		}
		return nil
	}

	return fmt.Errorf("expected string or mapping node, got %T", node)
}

func mutationStatementFromString(str string) (MutationStatement, error) {
	res, err := AutoDowner.ParseAndGetDefault(str)
	if err != nil {
		return MutationStatement{}, fmt.Errorf("can't generate undo statement from %s: %w", str, err)
	}
	return MutationStatement{Up: str, Down: res}, nil
}

package mutations

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
)

type YamlMigration struct {
	Node    ast.Node
	Name    string          `yaml:"name"`
	Depends []string        `yaml:"depends"`
	Sql     []YamlStatement `yaml:"sql"`
	Meta    []YamlStatement `yaml:"meta"`
	Roles   []string        `yaml:"roles"`
}

type YamlStatement struct {
	Node ast.Node
	Up   string `yaml:"up"`
	Down string `yaml:"down"`
}

func (stm *YamlStatement) UnmarshalYAML(node ast.Node) error {
	stm.Node = node

	if node.Type() == ast.StringType || node.Type() == ast.LiteralType {
		var val string
		yaml.NodeToValue(node, &val)
		stmt, err := yamlStatementFromString(val)
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
				stm.Up = value.Value.String()
			} else if value.Key.String() == "down" {
				stm.Down = value.Value.String()
			}
		}
		return nil
	}

	return fmt.Errorf("expected string or mapping node, got %T", node)
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

func LoadYamlMutations(paths ...string) ([]YamlMigration, error) {
	var res []YamlMigration
	for _, path := range paths {
		if info, err := os.Stat(path); err != nil {
			return nil, err
		} else if info.IsDir() {
			files, err := os.ReadDir(path)
			if err != nil {
				return nil, err
			}
			for _, file := range files {
				if file.IsDir() {
					continue
				}
				if strings.HasPrefix(file.Name(), "_") {
					continue
				}
				muts, err := readYamlFile(filepath.Join(path, file.Name()))
				if err != nil {
					return nil, err
				}
				res = append(res, muts...)
			}
		} else {
			muts, err := readYamlFile(path)
			if err != nil {
				return nil, err
			}
			res = append(res, muts...)
		}
	}
	return res, nil
}

func CollectYamlMutations(muts []YamlMigration, filename string) error {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := yaml.NewEncoder(f)
	for _, mut := range muts {
		if err := enc.Encode(mut); err != nil {
			return err
		}
		if _, err := f.WriteString("---\n"); err != nil {
			return err
		}
	}
	return nil
}

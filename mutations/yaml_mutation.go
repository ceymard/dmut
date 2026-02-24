package mutations

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
)

type YamlMigrationFile map[string]*YamlMigration

func (ymf YamlMigrationFile) Roles() mapset.Set[string] {
	var res = mapset.NewSet[string]()
	for _, mut := range ymf {
		for _, role := range mut.Roles {
			res.Add(role)
		}
	}
	return res
}

func (ymf YamlMigrationFile) ToDbMutationMap() DbMutationMap {
	var res DbMutationMap = make(DbMutationMap)
	for _, mut := range ymf {
		res[mut.db_sql.Hash] = mut.db_sql
		res[mut.db_meta.Hash] = mut.db_meta
	}
	return res
}

func (mut *YamlMigration) AddParent(parent *YamlMigration) {
	if mut.parents.Contains(parent) {
		return
	}
	mut.parents.Add(parent)
	mut.db_sql.Parents = append(mut.db_sql.Parents, parent.db_sql.Hash)
	mut.db_meta.Parents = append(mut.db_meta.Parents, parent.db_meta.Hash)
	parent.children.Add(mut)
	parent.db_sql.Children = append(parent.db_sql.Children, mut.db_sql.Hash)
	parent.db_meta.Children = append(parent.db_meta.Children, mut.db_meta.Hash)
}

func (ymf YamlMigrationFile) ResolveDependencies() error {
	for _, mut := range ymf {
		// For dotted names, find if there are parents and add them automatically.
		split_name := strings.Split(mut.Name, ".")
		for i := 0; i < len(split_name)-1; i++ {
			parent_name := strings.Join(split_name[:i+1], ".")
			if parent, ok := ymf[parent_name]; ok {
				mut.AddParent(parent)
			}
		}

		// Explicit dependencies in needs
		for _, parent_name := range mut.Needs {
			if parent, ok := ymf[parent_name]; ok {
				mut.AddParent(parent)
			} else {
				return fmt.Errorf("dependency %s not found", parent_name)
			}

		}
	}
	// pp.Println(ymf)
	return nil
}

func (ymf YamlMigrationFile) AddMutation(name string, mut *YamlMigration) error {
	if _, ok := ymf[name]; ok {
		return fmt.Errorf("duplicate migration name: %s", name)
	}
	ymf[name] = mut
	mut.Name = name
	mut.children = mapset.NewSet[*YamlMigration]()
	mut.parents = mapset.NewSet[*YamlMigration]()

	sql_up, sql_down := splitStatements(mut.Sql)

	sql_mut := &DbMutation{
		Name:     mut.Name,
		File:     mut.File,
		Meta:     false,
		Children: make([]string, 0),
		Up:       sql_up,
		Down:     sql_down,
	}
	sql_mut.ComputeHash()
	mut.db_sql = sql_mut

	meta_up, meta_down := splitStatements(mut.Meta)
	meta_mut := &DbMutation{
		Name:     mut.Name,
		File:     mut.File,
		Meta:     true,
		Children: make([]string, 0),
		Up:       meta_up,
		Down:     meta_down,
	}
	meta_mut.ComputeHash()
	mut.db_meta = meta_mut

	mut.db_meta.Parents = append(mut.db_meta.Parents, mut.db_sql.Hash)
	mut.db_sql.Children = append(mut.db_sql.Children, mut.db_meta.Hash)

	return nil
}

type YamlMigration struct {
	Node ast.Node
	Name string `yaml:"name"`
	File string `yaml:"file"`

	Needs []string        `yaml:"needs"`
	Sql   []YamlStatement `yaml:"sql"`
	Meta  []YamlStatement `yaml:"meta"`
	Roles []string        `yaml:"roles"`

	children mapset.Set[*YamlMigration]
	parents  mapset.Set[*YamlMigration]
	db_sql   *DbMutation
	db_meta  *DbMutation
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
				yaml.NodeToValue(value.Value, &stm.Up)
			} else if value.Key.String() == "down" {
				yaml.NodeToValue(value.Value, &stm.Down)
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

func readYamlFile(file YamlMigrationFile, filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error reading file %s: %w", filename, err)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	for {
		var ym YamlMigrationFile = make(YamlMigrationFile)
		err = dec.Decode(&ym)
		if err == io.EOF {
			break // normal end of stream
		}

		if err != nil {
			return fmt.Errorf("error decoding yaml: %w", err)
		}
		for name, mut := range ym {
			if err := file.AddMutation(name, mut); err != nil {
				return err
			}
		}
	}
	return nil
}

func LoadYamlMutations(paths ...string) (YamlMigrationFile, error) {
	var res YamlMigrationFile = make(YamlMigrationFile)
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
				err = readYamlFile(res, filepath.Join(path, file.Name()))
				if err != nil {
					return nil, err
				}
			}
		} else {
			err = readYamlFile(res, path)
			if err != nil {
				return nil, err
			}
		}
	}

	if err := res.ResolveDependencies(); err != nil {
		return nil, err
	}

	return res, nil
}

func splitStatements(stmts []YamlStatement) ([]string, []string) {
	var up []string = make([]string, 0)
	var down []string = make([]string, 0)
	for _, stmt := range stmts {
		up = append(up, stmt.Up)
		down = append([]string{stmt.Down}, down...)
	}
	return up, down
}

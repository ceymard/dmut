package mutations

import (
	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
	"github.com/samber/oops"
)

type MutationStatement struct {
	Up   string `json:"up" yaml:"up"`
	Down string `json:"down" yaml:"down"`
}

func parseStatements(value ast.Node) (list []MutationStatement, err error) {
	list = []MutationStatement{}
	if seq, ok := value.(*ast.SequenceNode); ok {
		for _, node := range seq.Values {
			stmt, err := parseSingleStatement(node)
			if err != nil {
				return list, err
			}
			list = append(list, stmt)
		}
		return list, nil
	} else {

	}
	return list, oops.In("mutations").Errorf("expected sequence, got %T", value)
}

func parseSingleStatement(value ast.Node) (stmt MutationStatement, err error) {
	var single string
	if err := yaml.NodeToValue(value, &single); err == nil {
		return mutationStatementFromString(single)
	}

	if err := yaml.NodeToValue(value, &stmt); err == nil {
		return stmt, nil
	}

	return stmt, oops.In("mutations").Errorf("expected string or mapping, got %T", value)
}

func mutationStatementFromString(str string) (MutationStatement, error) {
	res, err := AutoDowner.ParseAndGetDefault(str)
	if err != nil {
		return MutationStatement{}, oops.In("mutations").With("statement", str).Wrapf(err, "can't generate undo statement from %s", str)
	}
	if res == "" {
		return MutationStatement{}, oops.In("mutations").With("statement", str).Errorf("empty undo statement generated from %s", str)
	}
	return MutationStatement{Up: str, Down: res}, nil
}

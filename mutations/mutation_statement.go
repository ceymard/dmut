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

// parseStatements reads a core or glue block. validate, when not nil, is run on the SQL of
// every statement : the up text always, the down text only when it was written by hand
// (a generated down comes from an up that already passed).
func parseStatements(value ast.Node, validate func(sql string) error) (list []MutationStatement, err error) {
	list = []MutationStatement{}
	if seq, ok := value.(*ast.SequenceNode); ok {
		for i, node := range seq.Values {
			stmt, hand_written, err := parseSingleStatement(node)
			if err != nil {
				return list, err
			}
			if validate != nil {
				for _, sql := range []string{stmt.Up, handDown(stmt, hand_written)} {
					if err := validate(sql); err != nil {
						return list, oops.In("mutations").With("statement_index", i+1).Wrap(err)
					}
				}
			}
			list = append(list, stmt)
		}
		return list, nil
	} else {

	}
	return list, oops.In("mutations").Errorf("expected sequence, got %T", value)
}

// handDown returns the down text to validate : empty for a generated one.
func handDown(stmt MutationStatement, hand_written bool) string {
	if hand_written {
		return stmt.Down
	}
	return ""
}

// parseSingleStatement reports whether the statement was a mapping, i.e. its down is hand-written.
func parseSingleStatement(value ast.Node) (stmt MutationStatement, hand_written bool, err error) {
	var single string
	if err := yaml.NodeToValue(value, &single); err == nil {
		stmt, err := mutationStatementFromString(single)
		return stmt, false, err
	}

	if err := yaml.NodeToValue(value, &stmt); err == nil {
		return stmt, true, nil
	}

	return stmt, false, oops.In("mutations").Errorf("expected string or mapping, got %T", value)
}

func mutationStatementFromString(str string) (MutationStatement, error) {
	res, no_down, err := AutoDowner.ParseAndGetDefault(str)
	if err != nil {
		return MutationStatement{}, oops.In("mutations").With("statement", str).Wrapf(err, "can't generate undo statement from %s", str)
	}
	if no_down {
		// The statement has no reverse by design, not because we failed to find one.
		return MutationStatement{Up: str, Down: ""}, nil
	}
	if res == "" {
		return MutationStatement{}, oops.In("mutations").With("statement", str).Errorf("empty undo statement generated from %s", str)
	}
	return MutationStatement{Up: str, Down: res}, nil
}

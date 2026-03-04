package mutations

import (
	"github.com/samber/oops"
)

type MutationStatement struct {
	Up   string `yaml:"up"`
	Down string `yaml:"down"`
}

func parseStatements(value interface{}) (list []MutationStatement, err error) {
	if v, ok := value.([]interface{}); ok {
		for _, value := range v {
			stmt, err := parseSingleStatement(value)
			if err != nil {
				return list, err
			}
			list = append(list, stmt)
		}
		return list, nil
	} else {
		stmt, err := parseSingleStatement(value)
		if err != nil {
			return nil, err
		}
		return []MutationStatement{stmt}, nil
	}
}

func parseSingleStatement(value interface{}) (stmt MutationStatement, err error) {
	if v, ok := value.(string); ok {
		// simple string
		return mutationStatementFromString(v)
	} else if v, ok := value.(map[string]interface{}); ok {
		for key, value := range v {
			switch key {
			case "up":
				if v, ok := value.(string); ok {
					stmt.Up = v
				} else {
					return stmt, oops.In("mutations").Errorf("up must be a string")
				}
			case "down":
				if v, ok := value.(string); ok {
					stmt.Down = v
				} else {
					return stmt, oops.In("mutations").Errorf("down must be a string")
				}
			default:
				return stmt, oops.In("mutations").Errorf("unknown key %s", key)
			}
		}
		return stmt, nil
	}
	return stmt, oops.In("mutations").Errorf("expected string or mapping, got %T", value)
}

func mutationStatementFromString(str string) (MutationStatement, error) {
	res, err := AutoDowner.ParseAndGetDefault(str)
	if err != nil {
		return MutationStatement{}, oops.In("mutations").With("statement", str).Wrapf(err, "can't generate undo statement from %s", str)
	}
	return MutationStatement{Up: str, Down: res}, nil
}

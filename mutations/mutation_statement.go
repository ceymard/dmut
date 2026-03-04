package mutations

import (
	"fmt"
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
					return stmt, fmt.Errorf("up must be a string")
				}
			case "down":
				if v, ok := value.(string); ok {
					stmt.Down = v
				} else {
					return stmt, fmt.Errorf("down must be a string")
				}
			default:
				return stmt, fmt.Errorf("unknown key %s", key)
			}
		}
		return stmt, nil
	}
	return stmt, fmt.Errorf("expected string or mapping, got %T", value)
}

func mutationStatementFromString(str string) (MutationStatement, error) {
	res, err := AutoDowner.ParseAndGetDefault(str)
	if err != nil {
		return MutationStatement{}, fmt.Errorf("can't generate undo statement from %s: %w", str, err)
	}
	return MutationStatement{Up: str, Down: res}, nil
}

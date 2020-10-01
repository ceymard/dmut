package dmutparser

import (
	"fmt"
	"strings"
)

func (stmt *ASTStatement) Up(fileContents string) string {
	if stmt.UpOrDownStmt != nil {
		return stmt.UpOrDownStmt.Up()
	}
	// var str = ""
	// for _, tok := range stmt.Tokens {
	// 	str = str + tok.Value
	// }
	// the rest is to be sent as is.
	var start = stmt.Tokens[0]
	var end = stmt.Tokens[len(stmt.Tokens)-1]
	var cts = fileContents[start.Pos.Offset : end.Pos.Offset+len(end.Value)]
	return cts
}

func (stmt *ASTStatement) Down() string {
	if stmt.CreateStatement != nil {
		return stmt.CreateStatement.Down()
	} else if stmt.GrantStatement != nil {
		return stmt.GrantStatement.Down()
	} else if stmt.UpOrDownStmt != nil {
		return stmt.UpOrDownStmt.Down()
	} else if stmt.RlsStatement != nil {
		return stmt.RlsStatement.Down()
	}
	panic("Not implemented")
}

func (rls *RlsStatement) Down() string {
	return fmt.Sprintf(`ALTER TABLE %s DISABLE ROW LEVEL SECURITY;`, *rls.Table)
}

func (grant *GrantStatement) Down() string {
	return fmt.Sprintf(`REVOKE %s FROM %s;`, strings.Join(*grant.Perms, " "), *grant.To)
}

func (create *CreateStatement) Down() string {
	if create.Simple != nil {
		return create.Simple.Down()
	} else if create.Function != nil {
		return create.Function.Down()
	} else if create.PolicyOrTrigger != nil {
		return create.PolicyOrTrigger.Down()
	} else if create.Index != nil {
		return create.Index.Down()
	}
	panic("not implemented")
}

func (ind *CreateIndexStatement) Down() string {
	var splt = strings.Split(*ind.Table, ".")
	var schema = ""
	if len(splt) > 1 {
		schema = splt[0] + "."
	}
	return fmt.Sprintf(`DROP INDEX %s%s`, schema, *ind.Name)
}

func (pol *CreatePolicyStmt) Down() string {
	return fmt.Sprintf(`DROP %s %s ON %s;`, *pol.Kind, *pol.Name, *pol.Target)
}

func (fun *CreateFunctionStatement) Down() string {
	var args = ""
	if fun.Args != nil {
		args = strings.Join(*fun.Args, " ")
	}
	return fmt.Sprintf(`DROP FUNCTION %s (%s)`, *fun.Name, args)
}

func (simple *SimpleCreateStatement) Down() string {
	return fmt.Sprintf(`DROP %s %s;`, strings.Join(*simple.Kind, " "), *simple.Id)
}

func (ud *UpOrDownStmt) Up() string {
	if *ud.Kind == "down" {
		return ""
	}
	return ""
}

func (ud *UpOrDownStmt) Down() string {
	if *ud.Kind == "up" {
		return ""
	}
	return ""
}

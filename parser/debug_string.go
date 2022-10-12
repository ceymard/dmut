package dmutparser

import (
	"fmt"
	"strings"
)

func (c *CreateStatement) String() string {
	if c.Simple != nil {
		return c.Simple.String()
	}
	return "-create statement ?-"
}

func (cs *SimpleCreateStatement) String() string {
	return fmt.Sprint("create ", strings.Join(*cs.Kind, " "), " ", *cs.Id, " ")
}

func (m *MutationDecl) String() string {
	out := *m.Name
	if m.DependsOn != nil {
		out += " depends on "
		strs := make([]string, 0, 1)
		for _, d := range *m.DependsOn {
			name := d.Name
			if d.Starred != nil {
				name += ".*"
			}
			strs = append(strs, d.Name)
		}
		out += strings.Join(strs, ", ")
	}
	if m.Statements != nil {
		for _, s := range *m.Statements {
			out += " " + s.String() + "\n"
		}
	}
	return out
}

func (s *ASTStatement) String() string {
	if s.UpOrDownStmt != nil {
		return s.UpOrDownStmt.String()
	} else if s.CreateStatement != nil {
		return s.CreateStatement.String()
	}
	return "-statement-?"
}

func (s *UpOrDownStmt) String() string {
	return *s.Kind + ": " + *s.Stmt
}

func (t *TopLevel) String() string {
	muts := ""
	for _, m := range t.Decls {
		muts += m.String() + "\n"
	}
	return "Decls:\n" + muts
}

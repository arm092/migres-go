package schema

import (
	"fmt"

	"github.com/arm092/migres-go/internal/config"
	"github.com/arm092/migres-go/internal/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

type AlterStep struct {
	Kind    string
	Name    string
	NewName string
	Column  model.Column
}

// AlterSteps preserves the SQL operation semantics; DROP+ADD is never a rename.
func AlterSteps(sql string, c config.Config) ([]AlterStep, error) {
	n, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, err
	}
	x, ok := n.(*ast.AlterTableStmt)
	if !ok {
		return nil, fmt.Errorf("ALTER expected")
	}
	var out []AlterStep
	for _, sp := range x.Specs {
		switch sp.Tp {
		case ast.AlterTableAddColumns:
			for _, def := range sp.NewColumns {
				col, err := column(def, c)
				if err != nil {
					return nil, err
				}
				out = append(out, AlterStep{Kind: "add", Name: col.Name, Column: col})
			}
		case ast.AlterTableDropColumn:
			out = append(out, AlterStep{Kind: "drop", Name: sp.OldColumnName.Name.O})
		case ast.AlterTableRenameColumn:
			out = append(out, AlterStep{Kind: "rename", Name: sp.OldColumnName.Name.O, NewName: sp.NewColumnName.Name.O})
		case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
			for _, def := range sp.NewColumns {
				col, err := column(def, c)
				if err != nil {
					return nil, err
				}
				old := col.Name
				if sp.OldColumnName != nil {
					old = sp.OldColumnName.Name.O
				}
				if old != col.Name {
					out = append(out, AlterStep{Kind: "rename", Name: old, NewName: col.Name})
				}
				out = append(out, AlterStep{Kind: "modify", Name: col.Name, Column: col})
			}
		}
	}
	return out, nil
}

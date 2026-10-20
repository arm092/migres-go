package schema

import (
	"fmt"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// AlterWithoutRename keeps column specifications in their source order. The
// destination name is published separately by the durable target rename plan.
func AlterWithoutRename(sql string) (string, error) {
	n, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return "", err
	}
	x, ok := n.(*ast.AlterTableStmt)
	if !ok {
		return "", fmt.Errorf("expected ALTER TABLE")
	}
	var specs []*ast.AlterTableSpec
	for _, sp := range x.Specs {
		if sp.Tp != ast.AlterTableRenameTable {
			specs = append(specs, sp)
		}
	}
	if len(specs) == 0 {
		return "", fmt.Errorf("compound ALTER has no column specifications")
	}
	x.Specs = specs
	return restore(x)
}

package schema

import (
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

type TableReference struct{ Database, Table string }

// SHOW BINLOG EVENTS prefixes queries with USE. Parse both statements so gap
// validation can distinguish selected tables from unrelated database traffic.
func QueryTables(sql, defaultDatabase string) ([]TableReference, error) {
	nodes, _, err := parser.New().ParseSQL(sql)
	if err != nil {
		return nil, err
	}
	v := &tableReferences{database: defaultDatabase}
	for _, n := range nodes {
		if use, ok := n.(*ast.UseStmt); ok {
			v.database = use.DBName
		}
		n.Accept(v)
	}
	return v.tables, nil
}

type tableReferences struct {
	database string
	tables   []TableReference
}

func (v *tableReferences) Enter(n ast.Node) (ast.Node, bool) {
	if t, ok := n.(*ast.TableName); ok {
		db := t.Schema.O
		if db == "" {
			db = v.database
		}
		v.tables = append(v.tables, TableReference{Database: db, Table: t.Name.O})
	}
	return n, false
}
func (v *tableReferences) Leave(n ast.Node) (ast.Node, bool) { return n, true }

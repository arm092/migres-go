package schema

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/arm092/migres-go/internal/config"
	"github.com/arm092/migres-go/internal/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

func Quote(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
func Literal(s string) string {
	return "'" + strings.NewReplacer("\\", "\\\\", "'", "\\'").Replace(s) + "'"
}
func Ref(db, table string) string { return Quote(db) + "." + Quote(table) }

var precision = regexp.MustCompile(`\((\d+)(?:,\s*(\d+))?\)`)

func Type(mysqlType string, nullable bool, timezone string, low bool) (string, error) {
	raw := strings.ToLower(mysqlType)
	base := strings.Fields(raw)[0]
	if n := strings.IndexByte(base, '('); n >= 0 {
		base = base[:n]
	}
	var out string
	ints := map[string]int{"tinyint": 8, "smallint": 16, "mediumint": 32, "int": 32, "integer": 32, "bigint": 64}
	if bits, ok := ints[base]; ok {
		prefix := "Int"
		if strings.Contains(raw, "unsigned") {
			prefix = "UInt"
		}
		out = fmt.Sprintf("%s%d", prefix, bits)
	} else {
		switch base {
		case "float":
			out = "Float32"
		case "double", "real":
			out = "Float64"
		case "decimal", "numeric":
			m := precision.FindStringSubmatch(raw)
			if len(m) < 3 || m[2] == "" {
				return "", fmt.Errorf("invalid decimal type %q", mysqlType)
			}
			out = "Decimal(" + m[1] + "," + m[2] + ")"
		case "date":
			out = "Date32"
		case "datetime", "timestamp":
			p := "0"
			if m := precision.FindStringSubmatch(raw); len(m) > 1 {
				p = m[1]
			}
			out = "DateTime64(" + p + ", " + Literal(timezone) + ")"
		case "year":
			out = "UInt16"
		case "bit":
			out = "UInt64"
		case "char", "varchar", "text", "tinytext", "mediumtext", "longtext", "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "json", "enum", "set", "time":
			out = "String"
			if low && (base == "enum" || base == "set" || base == "char" || base == "varchar") {
				out = "LowCardinality(String)"
			}
		default:
			return "", fmt.Errorf("unsupported MySQL type %q", mysqlType)
		}
	}
	if nullable {
		if strings.HasPrefix(out, "LowCardinality(") {
			out = "LowCardinality(Nullable(String))"
		} else {
			out = "Nullable(" + out + ")"
		}
	}
	return out, nil
}
func Create(s model.Schema, c config.Config) (string, error) {
	if len(s.Keys) == 0 {
		return "", fmt.Errorf("%s has no unique non-null key", s.Table)
	}
	cols := []string{}
	for _, col := range s.Columns {
		if strings.HasPrefix(col.Name, "_migres_") {
			return "", fmt.Errorf("reserved column %s", col.Name)
		}
		def := Quote(col.Name) + " " + col.CHType
		if expr := Default(col); expr != "" {
			def += " DEFAULT " + expr
		}
		cols = append(cols, def)
	}
	cols = append(cols, "`_migres_version` UInt64", "`_migres_deleted` UInt8")
	keys := make([]string, len(s.Keys))
	for i, k := range s.Keys {
		keys[i] = Quote(k)
	}
	engine := "ReplacingMergeTree(`_migres_version`)"
	cluster := ""
	switch c.ClickHouse.Engine {
	case "ReplicatedReplacingMergeTree":
		if c.ClickHouse.Cluster == "" {
			return "", fmt.Errorf("cluster required")
		}
		cluster = " ON CLUSTER " + Quote(c.ClickHouse.Cluster)
		path := strings.TrimRight(c.ClickHouse.Replication.PathPrefix, "/") + "/" + c.ClickHouse.Database + "/" + s.Generation
		engine = "ReplicatedReplacingMergeTree(" + Literal(path) + ", " + Literal("{"+c.ClickHouse.Replication.ReplicaMacro+"}") + ", `_migres_version`)"
	case "MergeTree":
		engine = "MergeTree()"
	}
	return "CREATE TABLE IF NOT EXISTS " + Ref(c.ClickHouse.Database, s.Table) + cluster + " (" + strings.Join(cols, ", ") + ") ENGINE = " + engine + " ORDER BY (" + strings.Join(keys, ", ") + ") COMMENT " + Literal("migres:"+s.Generation), nil
}

type DDL struct {
	Kind     string
	Tables   []string
	Pairs    [][2]string
	Database string
}

func Parse(sql string) (DDL, error) {
	n, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return DDL{}, fmt.Errorf("parse MySQL DDL: %w", err)
	}
	var d DDL
	switch x := n.(type) {
	case *ast.CreateTableStmt:
		d.Kind = "create"
		d.Tables = []string{x.Table.Name.O}
		d.Database = x.Table.Schema.O
	case *ast.DropTableStmt:
		d.Kind = "drop"
		for _, t := range x.Tables {
			if d.Database != "" && t.Schema.O != "" && d.Database != t.Schema.O {
				return d, fmt.Errorf("cross-database DROP requires separate statements")
			}
			if t.Schema.O != "" {
				d.Database = t.Schema.O
			}
			d.Tables = append(d.Tables, t.Name.O)
		}
	case *ast.TruncateTableStmt:
		d.Kind = "truncate"
		d.Tables = []string{x.Table.Name.O}
		d.Database = x.Table.Schema.O
	case *ast.RenameTableStmt:
		d.Kind = "rename"
		for _, p := range x.TableToTables {
			if p.OldTable.Schema.O != "" && p.NewTable.Schema.O != "" && p.OldTable.Schema.O != p.NewTable.Schema.O {
				return d, fmt.Errorf("cross-database RENAME is unsupported")
			}
			if p.OldTable.Schema.O != "" {
				d.Database = p.OldTable.Schema.O
			}
			d.Pairs = append(d.Pairs, [2]string{p.OldTable.Name.O, p.NewTable.Name.O})
			d.Tables = append(d.Tables, p.OldTable.Name.O)
		}
	case *ast.AlterTableStmt:
		d.Kind = "alter"
		d.Tables = []string{x.Table.Name.O}
		d.Database = x.Table.Schema.O
		for _, spec := range x.Specs {
			if spec.Tp == ast.AlterTableRenameTable {
				if spec.NewTable.Schema.O != "" && d.Database != "" && spec.NewTable.Schema.O != d.Database {
					return d, fmt.Errorf("cross-database ALTER RENAME is unsupported")
				}
				if len(d.Pairs) != 0 {
					return d, fmt.Errorf("multiple table renames in ALTER are unsupported")
				}
				if len(x.Specs) == 1 {
					d.Kind = "rename"
				}
				d.Pairs = [][2]string{{x.Table.Name.O, spec.NewTable.Name.O}}
			}
		}
	default:
		return DDL{}, fmt.Errorf("unsupported DDL %T", n)
	}
	return d, nil
}

// ReferencesDatabase includes explicitly qualified names even when the query's
// default database points elsewhere. This also fences mixed-database DDL.
func ReferencesDatabase(sql, defaultDatabase, wanted string) (bool, error) {
	n, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return false, err
	}
	v := &databaseVisitor{defaultDatabase: defaultDatabase, wanted: wanted, databases: map[string]bool{}}
	n.Accept(v)
	if len(v.databases) > 1 {
		return v.found, fmt.Errorf("mixed-database DDL requires separate statements")
	}
	return v.found, nil
}

type databaseVisitor struct {
	defaultDatabase, wanted string
	found                   bool
	databases               map[string]bool
}

func (v *databaseVisitor) Enter(n ast.Node) (ast.Node, bool) {
	var database string
	switch x := n.(type) {
	case *ast.AlterDatabaseStmt:
		database = x.Name.O
		if database == "" {
			database = v.defaultDatabase
		}
	case *ast.DropDatabaseStmt:
		database = x.Name.O
	}
	if database != "" {
		v.databases[database] = true
		if database == v.wanted {
			v.found = true
		}
	}

	if t, ok := n.(*ast.TableName); ok {
		db := t.Schema.O
		if db == "" {
			db = v.defaultDatabase
		}
		if db == v.wanted {
			v.found = true
		}
		v.databases[db] = true
	}
	return n, false
}
func (v *databaseVisitor) Leave(n ast.Node) (ast.Node, bool) { return n, true }

package schema

import (
	"fmt"
	"slices"
	"strings"

	"github.com/arm092/migres-go/internal/charset"
	"github.com/arm092/migres-go/internal/config"
	"github.com/arm092/migres-go/internal/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
)

func restore(n ast.Node) (string, error) {
	var b strings.Builder
	err := n.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &b))
	return b.String(), err
}
func column(def *ast.ColumnDef, c config.Config, defaultCharset string) (model.Column, error) {
	var b strings.Builder
	if err := def.Tp.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &b)); err != nil {
		return model.Column{}, err
	}
	out := model.Column{Name: def.Name.Name.O, MySQLType: b.String(), Nullable: true}
	out.Charset = def.Tp.GetCharset()
	for _, option := range def.Options {
		if option.Tp == ast.ColumnOptionCollate && option.StrValue != "" {
			out.Charset = strings.SplitN(option.StrValue, "_", 2)[0]
		}
	}
	if out.Charset == "" && textType(out.MySQLType) {
		out.Charset = defaultCharset
	}
	if out.Charset != "" && out.Charset != "binary" {
		if err := charset.Validate(out.Charset); err != nil {
			return out, err
		}
	}
	out.Elements = append([]string(nil), def.Tp.GetElems()...)
	for _, o := range def.Options {
		switch o.Tp {
		case ast.ColumnOptionNotNull, ast.ColumnOptionPrimaryKey:
			out.Nullable = false
		case ast.ColumnOptionDefaultValue:
			if err := setDefault(&out, o.Expr); err != nil {
				return out, err
			}
		case ast.ColumnOptionGenerated:
			out.Generated = true
		}
	}
	typ, err := Type(out.MySQLType, out.Nullable, c.Migration.Timezone, c.Migration.LowCardinality)
	out.CHType = typ
	return out, err
}
func FromCreate(sql string, db string, generation string, c config.Config, defaults ...string) (model.Schema, error) {
	n, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return model.Schema{}, err
	}
	x, ok := n.(*ast.CreateTableStmt)
	if !ok {
		return model.Schema{}, fmt.Errorf("expected CREATE TABLE")
	}
	s := model.Schema{Database: db, Table: x.Table.Name.O, Generation: generation, SourceDDL: sql}
	s.Charset = "utf8mb4"
	if len(defaults) > 0 && defaults[0] != "" {
		s.Charset = defaults[0]
	}
	for _, o := range x.Options {
		if o.Tp == ast.TableOptionCharset {
			s.Charset = o.StrValue
		}
		if o.Tp == ast.TableOptionCollate && o.StrValue != "" {
			s.Charset = strings.SplitN(o.StrValue, "_", 2)[0]
		}
	}
	for _, def := range x.Cols {
		col, e := column(def, c, s.Charset)
		if e != nil {
			return s, e
		}
		s.Columns = append(s.Columns, col)
		for _, o := range def.Options {
			if o.Tp == ast.ColumnOptionPrimaryKey {
				s.Keys = append(s.Keys, col.Name)
			}
		}
	}
	for _, constraint := range x.Constraints {
		if constraint.Tp == ast.ConstraintPrimaryKey {
			s.Keys = nil
			for _, k := range constraint.Keys {
				s.Keys = append(s.Keys, k.Column.Name.O)
			}
		}
	}
	if custom := c.MySQL.Keys[s.Table]; len(custom) > 0 {
		primary := append([]string(nil), s.Keys...)
		for i := range s.Columns {
			if slices.Contains(primary, s.Columns[i].Name) {
				s.Columns[i].Nullable = false
			}
		}
		valid := slices.Equal(primary, custom)
		for _, constraint := range x.Constraints {
			switch constraint.Tp {
			case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
				var cols []string
				full := true
				for _, part := range constraint.Keys {
					if part.Column == nil || part.Length > 0 {
						full = false
						break
					}
					cols = append(cols, part.Column.Name.O)
				}
				if full && slices.Equal(cols, custom) {
					valid = true
				}
			}
		}
		if len(custom) == 1 {
			for _, def := range x.Cols {
				if def.Name.Name.O == custom[0] {
					for _, o := range def.Options {
						if o.Tp == ast.ColumnOptionUniqKey {
							valid = true
						}
					}
				}
			}
		}
		if !valid {
			return s, fmt.Errorf("configured key for %s is not a full UNIQUE constraint", s.Table)
		}
		for _, name := range custom {
			found := false
			for _, col := range s.Columns {
				if col.Name == name {
					found = true
					if col.Nullable {
						return s, fmt.Errorf("configured key %s is nullable", name)
					}
				}
			}
			if !found {
				return s, fmt.Errorf("configured key column missing")
			}
		}
		s.Keys = slices.Clone(custom)
	}
	for i, col := range s.Columns {
		if slices.Contains(s.Keys, col.Name) {
			s.Columns[i].Nullable = false
			s.Columns[i].CHType, err = Type(col.MySQLType, false, c.Migration.Timezone, c.Migration.LowCardinality)
			if err != nil {
				return s, err
			}
		}
	}
	if len(s.Keys) == 0 {
		return s, fmt.Errorf("table %s lacks a configured key", s.Table)
	}
	return s, nil
}

// Apply advances a persisted schema using the actual binlog SQL, never live metadata.
func Apply(old model.Schema, sql string, c config.Config) (model.Schema, bool, error) {
	n, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return old, false, err
	}
	x, ok := n.(*ast.AlterTableStmt)
	if !ok {
		return old, false, fmt.Errorf("expected ALTER TABLE")
	}
	s := old
	s.Columns = slices.Clone(old.Columns)
	s.Keys = slices.Clone(old.Keys)
	rebuild := false
	// Table defaults apply to all new/modified columns in a compound ALTER,
	// irrespective of where DEFAULT CHARSET appears in its SQL specification.
	for _, spec := range x.Specs {
		if spec.Tp != ast.AlterTableOption {
			continue
		}
		for _, o := range spec.Options {
			if o.Tp == ast.TableOptionCharset {
				s.Charset = o.StrValue
			}
			if o.Tp == ast.TableOptionCollate && o.StrValue != "" {
				s.Charset = strings.SplitN(o.StrValue, "_", 2)[0]
			}
		}
	}
	index := func(name string) int {
		for i, col := range s.Columns {
			if col.Name == name {
				return i
			}
		}
		return -1
	}
	insert := func(col model.Column, pos *ast.ColumnPosition) error {
		at := len(s.Columns)
		if pos != nil {
			switch pos.Tp {
			case ast.ColumnPositionFirst:
				at = 0
			case ast.ColumnPositionAfter:
				i := index(pos.RelativeColumn.Name.O)
				if i < 0 {
					return fmt.Errorf("AFTER references unknown column")
				}
				at = i + 1
			}
		}
		s.Columns = slices.Insert(s.Columns, at, col)
		return nil
	}
	for _, spec := range x.Specs {
		switch spec.Tp {
		case ast.AlterTableAddColumns:
			for _, def := range spec.NewColumns {
				col, e := column(def, c, s.Charset)
				if e != nil {
					return s, false, e
				}
				if col.DefaultExpression || col.Generated {
					rebuild = true
				}
				if index(col.Name) >= 0 {
					continue
				}
				if err = insert(col, spec.Position); err != nil {
					return s, false, err
				}
			}
		case ast.AlterTableAlterColumn:
			for _, def := range spec.NewColumns {
				i := index(def.Name.Name.O)
				if i < 0 {
					return s, false, fmt.Errorf("unknown default column %s", def.Name.Name.O)
				}
				s.Columns[i].Default = nil
				s.Columns[i].DefaultExpression = false
				s.Columns[i].DefaultSQL = ""
				for _, option := range def.Options {
					if option.Expr != nil {
						if err := setDefault(&s.Columns[i], option.Expr); err != nil {
							return s, false, err
						}
					}
				}
			}
		case ast.AlterTableDropColumn:
			i := index(spec.OldColumnName.Name.O)
			if i < 0 {
				return s, false, fmt.Errorf("unknown dropped column")
			}
			if slices.Contains(s.Keys, s.Columns[i].Name) {
				rebuild = true
				s.Keys = slices.DeleteFunc(s.Keys, func(k string) bool { return k == s.Columns[i].Name })
			}
			s.Columns = slices.Delete(s.Columns, i, i+1)
		case ast.AlterTableRenameColumn:
			i := index(spec.OldColumnName.Name.O)
			if i < 0 {
				return s, false, fmt.Errorf("unknown renamed column")
			}
			oldName := s.Columns[i].Name
			s.Columns[i].Name = spec.NewColumnName.Name.O
			for j, k := range s.Keys {
				if k == oldName {
					s.Keys[j] = s.Columns[i].Name
					rebuild = true
				}
			}
		case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
			for _, def := range spec.NewColumns {
				name := def.Name.Name.O
				if spec.OldColumnName != nil {
					name = spec.OldColumnName.Name.O
				}
				i := index(name)
				if i < 0 {
					return s, false, fmt.Errorf("unknown modified column")
				}
				col, e := column(def, c, s.Charset)
				if e != nil {
					return s, false, e
				}
				if col.CHType != s.Columns[i].CHType || col.Charset != s.Columns[i].Charset {
					rebuild = true
				}
				for j, k := range s.Keys {
					if k == name {
						if len(c.MySQL.Keys[s.Table]) > 0 && col.Nullable {
							return s, false, fmt.Errorf("configured identity columns must remain explicitly NOT NULL")
						}
						s.Keys[j] = col.Name
						col.Nullable = false
						col.CHType, e = Type(col.MySQLType, false, c.Migration.Timezone, c.Migration.LowCardinality)
						if e != nil {
							return s, false, e
						}
						if name != col.Name {
							rebuild = true
						}
					}
				}
				s.Columns = slices.Delete(s.Columns, i, i+1)
				if spec.Position == nil || spec.Position.Tp == ast.ColumnPositionNone {
					s.Columns = slices.Insert(s.Columns, i, col)
				} else if err = insert(col, spec.Position); err != nil {
					return s, false, err
				}
			}
		case ast.AlterTableDropPrimaryKey:
			s.Keys = nil
			rebuild = true
		case ast.AlterTableAddConstraint:
			if spec.Constraint.Tp == ast.ConstraintPrimaryKey {
				s.Keys = nil
				for _, k := range spec.Constraint.Keys {
					s.Keys = append(s.Keys, k.Column.Name.O)
				}
				rebuild = true
			}
		case ast.AlterTableRenameTable:
			s.Table = spec.NewTable.Name.O
		case ast.AlterTableDropIndex:
			if len(c.MySQL.Keys[s.Table]) > 0 {
				return s, false, fmt.Errorf("dropping indexes with a configured identity requires key revalidation and resnapshot")
			}
		case ast.AlterTableOption:
			for _, o := range spec.Options {
				if o.Tp == ast.TableOptionCharset || o.Tp == ast.TableOptionCollate {
					name := o.StrValue
					if o.Tp == ast.TableOptionCollate {
						name = strings.SplitN(name, "_", 2)[0]
					}
					if name == "" {
						return s, false, fmt.Errorf("implicit server charset change requires resnapshot")
					}
					if err := charset.Validate(name); err != nil {
						return s, false, err
					}
					s.Charset = name
					if o.Tp == ast.TableOptionCharset && o.UintValue == ast.TableOptionCharsetWithConvertTo {
						for i := range s.Columns {
							if textType(s.Columns[i].MySQLType) {
								s.Columns[i].Charset = name
							}
						}
						rebuild = true
					}
				}
			}
		case ast.AlterTableDropForeignKey, ast.AlterTableRenameIndex, ast.AlterTableLock, ast.AlterTableAlgorithm:
		// No stored row shape change. Explicit key constraints are revalidated on rebuild.
		default:
			return s, false, fmt.Errorf("unsupported ALTER operation %d", spec.Tp)
		}
	}
	if custom := c.MySQL.Keys[s.Table]; len(custom) > 0 {
		s.Keys = slices.Clone(custom)
	}
	if len(s.Keys) == 0 {
		return s, false, fmt.Errorf("DDL removes unique identity; configure a replacement key and resnapshot")
	}
	for _, key := range s.Keys {
		i := index(key)
		if i < 0 {
			return s, false, fmt.Errorf("DDL removes configured identity column %s", key)
		}
		s.Columns[i].Nullable = false
		s.Columns[i].CHType, err = Type(s.Columns[i].MySQLType, false, c.Migration.Timezone, c.Migration.LowCardinality)
		if err != nil {
			return s, false, err
		}
	}
	return s, rebuild, nil
}

func textType(t string) bool {
	t = strings.ToLower(t)
	return strings.HasPrefix(t, "char") || strings.HasPrefix(t, "varchar") || strings.Contains(t, "text") || strings.HasPrefix(t, "enum(") || strings.HasPrefix(t, "set(")
}

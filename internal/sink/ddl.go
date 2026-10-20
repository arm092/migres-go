package sink

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/schema"
)

func (c *ClickHouse) DropOwned(ctx context.Context, s model.Schema) error {
	connections := []driver.Conn{c.Conn}
	if c.Config.ClickHouse.Engine == "ReplicatedReplacingMergeTree" {
		connections = nil
		for _, endpoint := range c.Config.ClickHouse.Addresses {
			cfg := c.Config
			cfg.ClickHouse.Addresses = []string{endpoint}
			peer, err := Open(cfg)
			if err != nil {
				return err
			}
			defer peer.Close()
			connections = append(connections, peer.Conn)
		}
	}
	// A previous cluster DROP may have succeeded on only some replicas.
	for _, conn := range connections {
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=? AND name=?", c.Config.ClickHouse.Database, s.Table).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			continue
		}
		var comment string
		if err := conn.QueryRow(ctx, "SELECT comment FROM system.tables WHERE database=? AND name=?", c.Config.ClickHouse.Database, s.Table).Scan(&comment); err != nil {
			return err
		}
		if comment != "migres:"+s.Generation {
			return fmt.Errorf("refusing to drop unowned table %s", s.Table)
		}
	}
	cluster := ""
	if c.Config.ClickHouse.Engine == "ReplicatedReplacingMergeTree" {
		cluster = " ON CLUSTER " + schema.Quote(c.Config.ClickHouse.Cluster)
	}
	if err := c.Conn.Exec(ctx, "DROP TABLE IF EXISTS "+schema.Ref(c.Config.ClickHouse.Database, s.Table)+cluster+" SYNC"); err != nil {
		return err
	}
	for _, conn := range connections {
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=? AND name=?", c.Config.ClickHouse.Database, s.Table).Scan(&n); err != nil {
			return err
		}
		if n != 0 {
			return fmt.Errorf("DROP incomplete on a replica for %s", s.Table)
		}
	}
	if c.Store != nil {
		return c.Store.Disown(s.Table)
	}
	return nil
}
func (c *ClickHouse) Alter(ctx context.Context, t model.Task) error {
	if t.Schema == nil || t.Previous == nil {
		return fmt.Errorf("ALTER lacks historical schemas")
	}
	if t.Rebuild {
		return fmt.Errorf("ALTER requires coordinated table rebuild")
	}
	current, err := c.columns(ctx, t.Table)
	if err != nil {
		return err
	}
	steps, err := schema.AlterSteps(t.SQL, c.Config)
	if err != nil {
		return err
	}
	cluster := ""
	if c.Config.ClickHouse.Engine == "ReplicatedReplacingMergeTree" {
		cluster = " ON CLUSTER " + schema.Quote(c.Config.ClickHouse.Cluster)
	}
	prefix := "ALTER TABLE " + schema.Ref(c.Config.ClickHouse.Database, t.Table) + cluster + " "
	for index, step := range steps {
		if c.Store == nil || t.Token == "" {
			return fmt.Errorf("ALTER requires a durable task identity")
		}
		marker := fmt.Sprintf("alter:%s:step:%d", t.Token, index)
		var completed bool
		if err := c.Store.Meta(marker, &completed); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if completed {
			continue
		}
		freeze := func(name, typ string) error {
			frozenMarker := marker + ":frozen"
			var frozen bool
			if e := c.Store.Meta(frozenMarker, &frozen); e != nil && !errors.Is(e, os.ErrNotExist) {
				return e
			}
			if frozen {
				return nil
			}
			if cluster == "" {
				if e := freezeColumn(ctx, c.Conn, c.Config.ClickHouse.Database, t.Table, name, typ); e != nil {
					return e
				}
			} else {
				for _, endpoint := range c.Config.ClickHouse.Addresses {
					cfg := c.Config
					cfg.ClickHouse.Addresses = []string{endpoint}
					peer, e := Open(cfg)
					if e != nil {
						return e
					}
					e = freezeColumn(ctx, peer.Conn, c.Config.ClickHouse.Database, t.Table, name, typ)
					peer.Close()
					if e != nil {
						return e
					}
				}
			}
			return c.Store.SetMeta(frozenMarker, true)
		}
		switch step.Kind {
		case "drop":
			if _, ok := current[step.Name]; ok || cluster != "" {
				if err = c.Conn.Exec(ctx, prefix+"DROP COLUMN IF EXISTS "+schema.Quote(step.Name)); err != nil {
					return err
				}
				delete(current, step.Name)
			}
		case "rename":
			if _, ok := current[step.Name]; ok || cluster != "" {
				if _, exists := current[step.NewName]; !exists || cluster != "" {
					if err = c.Conn.Exec(ctx, prefix+"RENAME COLUMN IF EXISTS "+schema.Quote(step.Name)+" TO "+schema.Quote(step.NewName)); err != nil {
						return err
					}
					current[step.NewName] = current[step.Name]
					delete(current, step.Name)
				}
			}
		case "default":
			typ, exists := current[step.Name]
			if !exists {
				return fmt.Errorf("default column %s is missing", step.Name)
			}
			if err = freeze(step.Name, typ); err != nil {
				return err
			}
			expr := schema.Default(step.Column)
			clause := "MODIFY COLUMN " + schema.Quote(step.Name) + " REMOVE DEFAULT"
			if expr != "" {
				clause = "MODIFY COLUMN " + schema.Quote(step.Name) + " " + typ + " DEFAULT " + expr
			}
			if expr == "" && cluster != "" {
				// REMOVE DEFAULT has no IF EXISTS form. Inspect each replica so
				// a partially applied cluster DDL remains replayable.
				for _, endpoint := range c.Config.ClickHouse.Addresses {
					cfg := c.Config
					cfg.ClickHouse.Addresses = []string{endpoint}
					peer, e := Open(cfg)
					if e != nil {
						return e
					}
					e = removeDefault(ctx, peer.Conn, c.Config.ClickHouse.Database, t.Table, step.Name)
					peer.Close()
					if e != nil {
						return e
					}
				}
			} else if expr == "" {
				if err = removeDefault(ctx, c.Conn, c.Config.ClickHouse.Database, t.Table, step.Name); err != nil {
					return err
				}
			} else if err = c.Conn.Exec(ctx, prefix+clause); err != nil {
				return err
			}

		case "add", "modify":
			col := step.Column
			for _, desired := range t.Schema.Columns {
				if desired.Name == col.Name {
					defaultValue, defaultSQL, expression := col.Default, col.DefaultSQL, col.DefaultExpression
					col = desired
					col.Default, col.DefaultSQL, col.DefaultExpression = defaultValue, defaultSQL, expression
					break
				}
			}
			def := ""
			if expr := schema.Default(col); expr != "" {
				def = " DEFAULT " + expr
			}
			_, exists := current[col.Name]
			if !exists || (cluster != "" && step.Kind == "add") {
				if err = c.Conn.Exec(ctx, prefix+"ADD COLUMN IF NOT EXISTS "+schema.Quote(col.Name)+" "+col.CHType+def); err != nil {
					return err
				}
			} else if step.Kind == "modify" {
				if err = freeze(col.Name, current[col.Name]); err != nil {
					return err
				}
				if err = c.Conn.Exec(ctx, prefix+"MODIFY COLUMN "+schema.Quote(col.Name)+" "+col.CHType+def); err != nil {
					return err
				}
				if def == "" {
					if cluster == "" {
						if err = removeDefault(ctx, c.Conn, c.Config.ClickHouse.Database, t.Table, col.Name); err != nil {
							return err
						}
					} else {
						for _, endpoint := range c.Config.ClickHouse.Addresses {
							cfg := c.Config
							cfg.ClickHouse.Addresses = []string{endpoint}
							peer, e := Open(cfg)
							if e != nil {
								return e
							}
							e = removeDefault(ctx, peer.Conn, c.Config.ClickHouse.Database, t.Table, col.Name)
							peer.Close()
							if e != nil {
								return e
							}
						}
					}
				}
			}
			if step.Kind == "add" {
				// Added columns otherwise keep a virtual default in old parts.
				// Freeze their initial values before a later SET/DROP DEFAULT.
				if err = freeze(col.Name, col.CHType); err != nil {
					return err
				}
			}
			current[col.Name] = col.CHType
		}
		// Persist each step before starting the next one. Names may be reused by
		// later ADDs, so replaying an earlier RENAME/DROP would be destructive.
		connections := []driver.Conn{c.Conn}
		var peers []*ClickHouse
		for _, endpoint := range c.Config.ClickHouse.Addresses {
			cfg := c.Config
			cfg.ClickHouse.Addresses = []string{endpoint}
			peer, e := Open(cfg)
			if e != nil {
				for _, p := range peers {
					p.Close()
				}
				return e
			}
			peers = append(peers, peer)
			connections = append(connections, peer.Conn)
		}
		var verifyErr error
		for _, conn := range connections {
			probe := &ClickHouse{Conn: conn, Config: c.Config}
			cols, e := probe.columns(ctx, t.Table)
			if e != nil {
				verifyErr = e
				break
			}
			_, old := cols[step.Name]
			_, fresh := cols[step.NewName]
			switch step.Kind {
			case "drop":
				if old {
					verifyErr = fmt.Errorf("DROP COLUMN incomplete")
				}
			case "rename":
				if old || !fresh {
					verifyErr = fmt.Errorf("RENAME COLUMN incomplete")
				}
			case "add", "modify", "default":
				typ := current[step.Name]
				if typ == "" || cols[step.Name] != typ {
					verifyErr = fmt.Errorf("column type change incomplete for %s", step.Name)
				} else {
					verifyErr = verifyDefault(ctx, conn, c.Config.ClickHouse.Database, t.Table, step.Name, schema.Default(step.Column))
				}
			}
			if verifyErr != nil {
				break
			}
		}
		for _, p := range peers {
			p.Close()
		}
		if verifyErr != nil {
			return verifyErr
		}
		if err = c.Store.SetMeta(marker, true); err != nil {
			return err
		}
	}
	for _, endpoint := range c.Config.ClickHouse.Addresses {
		cfg := c.Config
		cfg.ClickHouse.Addresses = []string{endpoint}
		peer, e := Open(cfg)
		if e != nil {
			return e
		}
		cols, e := peer.columns(ctx, t.Table)
		peer.Close()
		if e != nil {
			return e
		}
		for _, col := range t.Schema.Columns {
			if cols[col.Name] != col.CHType {
				return fmt.Errorf("ALTER incomplete on replica %s", endpoint)
			}
		}
		for _, old := range t.Previous.Columns {
			found := false
			for _, col := range t.Schema.Columns {
				if col.Name == old.Name {
					found = true
					break
				}
			}
			if !found {
				if _, exists := cols[old.Name]; exists {
					return fmt.Errorf("ALTER left obsolete column on replica %s", endpoint)
				}
			}
		}
	}
	return nil
}
func (c *ClickHouse) columns(ctx context.Context, table string) (map[string]string, error) {
	rows, err := c.Conn.Query(ctx, "SELECT name,type FROM system.columns WHERE database=? AND table=?", c.Config.ClickHouse.Database, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err = rows.Scan(&name, &typ); err != nil {
			return nil, err
		}
		out[name] = typ
	}
	return out, rows.Err()
}

// REMOVE DEFAULT fails when absent; a lost response must replay as a no-op.
func removeDefault(ctx context.Context, conn driver.Conn, database, table, column string) error {
	var kind string
	if err := conn.QueryRow(ctx, "SELECT default_kind FROM system.columns WHERE database=? AND table=? AND name=?", database, table, column).Scan(&kind); err != nil {
		return err
	}
	if kind == "" {
		return nil
	}
	return conn.Exec(ctx, "ALTER TABLE "+schema.Ref(database, table)+" MODIFY COLUMN "+schema.Quote(column)+" REMOVE DEFAULT")
}

// Freeze virtual values before changing their expression. MATERIALIZE on supported
// ClickHouse versions preserves values explicitly stored by snapshot/row events.
func freezeColumn(ctx context.Context, conn driver.Conn, database, table, column, typ string) error {
	var kind string
	if err := conn.QueryRow(ctx, "SELECT default_kind FROM system.columns WHERE database=? AND table=? AND name=?", database, table, column).Scan(&kind); err != nil {
		return err
	}
	prefix := "ALTER TABLE " + schema.Ref(database, table) + " MODIFY COLUMN " + schema.Quote(column)
	if kind == "" {
		if err := conn.Exec(ctx, prefix+" "+typ+" DEFAULT defaultValueOfTypeName("+schema.Literal(typ)+")"); err != nil {
			return err
		}
	}
	if err := conn.Exec(ctx, "ALTER TABLE "+schema.Ref(database, table)+" MATERIALIZE COLUMN "+schema.Quote(column)+" SETTINGS mutations_sync=2"); err != nil {
		return err
	}
	if kind == "" {
		return removeDefault(ctx, conn, database, table, column)
	}
	return nil
}

// Parse both expressions with the target server's SQL formatter so metadata
// spelling differences (1.20 vs 1.2, quoting, whitespace) do not mask drift.
func verifyDefault(ctx context.Context, conn driver.Conn, database, table, column, expected string) error {
	var kind, expression string
	if err := conn.QueryRow(ctx, "SELECT default_kind,default_expression FROM system.columns WHERE database=? AND table=? AND name=?", database, table, column).Scan(&kind, &expression); err != nil {
		return err
	}
	if expected == "" {
		if kind != "" || expression != "" {
			return fmt.Errorf("DEFAULT removal incomplete for %s", column)
		}
		return nil
	}
	if kind != "DEFAULT" {
		return fmt.Errorf("DEFAULT kind incomplete for %s", column)
	}
	var actualCanonical, expectedCanonical string
	if err := conn.QueryRow(ctx, "SELECT formatQuerySingleLine(?),formatQuerySingleLine(?)", "SELECT "+expression, "SELECT "+expected).Scan(&actualCanonical, &expectedCanonical); err != nil {
		return err
	}
	if actualCanonical != expectedCanonical {
		return fmt.Errorf("DEFAULT expression incomplete for %s: got %s, expected %s", column, expression, expected)
	}
	return nil
}

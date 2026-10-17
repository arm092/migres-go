package sink

import (
	"context"
	"fmt"

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
		if err := c.Store.Meta(marker, &completed); err == nil && completed {
			continue
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
		case "add", "modify":
			col := step.Column
			for _, desired := range t.Schema.Columns {
				if desired.Name == col.Name {
					col = desired
					break
				}
			}
			def := ""
			if col.Default != nil {
				def = " DEFAULT " + schema.Literal(*col.Default)
			}
			_, exists := current[col.Name]
			if !exists || (cluster != "" && step.Kind == "add") {
				if err = c.Conn.Exec(ctx, prefix+"ADD COLUMN IF NOT EXISTS "+schema.Quote(col.Name)+" "+col.CHType+def); err != nil {
					return err
				}
			} else if step.Kind == "modify" {
				if err = c.Conn.Exec(ctx, prefix+"MODIFY COLUMN "+schema.Quote(col.Name)+" "+col.CHType+def); err != nil {
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
			case "add", "modify":
				if _, ok := cols[step.Column.Name]; !ok {
					verifyErr = fmt.Errorf("column change incomplete")
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

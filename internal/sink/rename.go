package sink

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/schema"
)

func (c *ClickHouse) Rename(ctx context.Context, pair [2]string, cluster string) error {
	if c.Store == nil {
		return fmt.Errorf("rename requires ownership journal")
	}
	owned, err := c.Store.Owned()
	if err != nil {
		return err
	}
	var sc *model.Schema
	for _, s := range owned {
		if s.Table == pair[0] {
			copy := s
			sc = &copy
			break
		}
	}
	if sc == nil {
		for _, s := range owned {
			if s.Table == pair[1] {
				return nil
			}
		}
		return fmt.Errorf("rename source is not owned")
	}
	connections := []driver.Conn{c.Conn}
	if cluster != "" {
		connections = nil
		for _, endpoint := range c.Config.ClickHouse.Addresses {
			cfg := c.Config
			cfg.ClickHouse.Addresses = []string{endpoint}
			peer, e := Open(cfg)
			if e != nil {
				return e
			}
			defer peer.Close()
			connections = append(connections, peer.Conn)
		}
	}
	marker := func(conn driver.Conn, name string) (string, uint64, error) {
		var m string
		var n uint64
		e := conn.QueryRow(ctx, "SELECT any(comment),count() FROM system.tables WHERE database=? AND name=?", c.Config.ClickHouse.Database, name).Scan(&m, &n)
		return m, n, e
	}
	pending := []driver.Conn{}
	for _, conn := range connections {
		old, n, e := marker(conn, pair[0])
		if e != nil {
			return e
		}
		fresh, nn, e := marker(conn, pair[1])
		if e != nil {
			return e
		}
		if n > 0 && old != "migres:"+sc.Generation || nn > 0 && fresh != "migres:"+sc.Generation || n+nn != 1 {
			return fmt.Errorf("rename ownership/state mismatch")
		}
		if n > 0 {
			pending = append(pending, conn)
		}
	}
	query := "RENAME TABLE " + schema.Ref(c.Config.ClickHouse.Database, pair[0]) + " TO " + schema.Ref(c.Config.ClickHouse.Database, pair[1])
	if len(pending) == len(connections) {
		if err = c.Conn.Exec(ctx, query+cluster); err != nil {
			return err
		}
	} else {
		// Recover a partially completed cluster command without rerunning it on
		// replicas that already renamed. Actual generation markers are the fence.
		for _, conn := range pending {
			if err = conn.Exec(ctx, query); err != nil {
				return err
			}
		}
	}
	for i, conn := range connections {
		m, n, e := marker(conn, pair[1])
		if e != nil {
			return e
		}
		if n != 1 || m != "migres:"+sc.Generation {
			return fmt.Errorf("rename destination ownership mismatch")
		}
		if err = c.Store.SetMeta(fmt.Sprintf("rename:%s:%d", sc.Generation, i), pair[1]); err != nil {
			return err
		}
	}
	sc.Table = pair[1]
	if err = c.Store.Own(*sc); err != nil {
		return err
	}
	return c.Store.Disown(pair[0])
}

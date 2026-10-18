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

// Persist pair identities before the first SQL statement. Names can be reused
// later in an atomic MySQL swap; retry must never rotate the completed swap again.
func (c *ClickHouse) RenameTask(ctx context.Context, t model.Task, pairs [][2]string, cluster string) error {
	if c.Store == nil || t.Token == "" {
		return fmt.Errorf("rename requires durable task identity")
	}
	type step struct {
		Pair       [2]string
		Generation string
	}
	meta := "renameplan:" + t.Token
	var plan []step
	if err := c.Store.Meta(meta, &plan); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		owned, err := c.Store.Owned()
		if err != nil {
			return err
		}
		byName := map[string]string{}
		for _, s := range owned {
			byName[s.Table] = s.Generation
		}
		for _, pair := range pairs {
			gen, ok := byName[pair[0]]
			if !ok {
				return fmt.Errorf("rename source is not owned")
			}
			if _, exists := byName[pair[1]]; exists {
				return fmt.Errorf("rename destination already exists in ownership")
			}
			plan = append(plan, step{pair, gen})
			delete(byName, pair[0])
			byName[pair[1]] = gen
		}
		if err = c.Store.SetMeta(meta, plan); err != nil {
			return err
		}
	}
	for i, st := range plan {
		marker := fmt.Sprintf("%s:%d", meta, i)
		var done bool
		err := c.Store.Meta(marker, &done)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if done {
			continue
		}
		owned, err := c.Store.Owned()
		if err != nil {
			return err
		}
		found := false
		for _, s := range owned {
			if (s.Table == st.Pair[0] || s.Table == st.Pair[1]) && s.Generation == st.Generation {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("rename pair has no owned generation")
		}
		for _, s := range owned {
			if s.Table == st.Pair[0] && s.Generation != st.Generation {
				return fmt.Errorf("rename pair generation changed")
			}
		}
		if err = c.Rename(ctx, st.Pair, cluster); err != nil {
			return err
		}
		if err = c.Store.SetMeta(marker, true); err != nil {
			return err
		}
	}
	return nil
}

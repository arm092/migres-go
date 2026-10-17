package sink

import (
	"context"
	"fmt"

	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/schema"
)

// Exchange checks generation markers at each endpoint before swapping, so a
// lost response or a partially completed cluster switch cannot toggle it back.
func (c *ClickHouse) Exchange(ctx context.Context, old, shadow, published model.Schema) error {
	endpoints := c.Config.ClickHouse.Addresses
	if len(endpoints) == 0 {
		endpoints = []string{""}
	}
	for _, endpoint := range endpoints {
		conn := c.Conn
		var peer *ClickHouse
		if endpoint != "" {
			cfg := c.Config
			cfg.ClickHouse.Addresses = []string{endpoint}
			var err error
			peer, err = Open(cfg)
			if err != nil {
				return err
			}
			conn = peer.Conn
		}
		var mainMarker string
		err := conn.QueryRow(ctx, "SELECT comment FROM system.tables WHERE database=? AND name=?", c.Config.ClickHouse.Database, old.Table).Scan(&mainMarker)
		if err == nil && mainMarker != "migres:"+published.Generation {
			if mainMarker != "migres:"+old.Generation {
				err = fmt.Errorf("unexpected generation at %s", endpoint)
			} else {
				var other string
				err = conn.QueryRow(ctx, "SELECT comment FROM system.tables WHERE database=? AND name=?", c.Config.ClickHouse.Database, shadow.Table).Scan(&other)
				if err == nil && other != "migres:"+published.Generation {
					err = fmt.Errorf("shadow generation mismatch")
				}
				if err == nil {
					err = conn.Exec(ctx, "EXCHANGE TABLES "+schema.Ref(c.Config.ClickHouse.Database, old.Table)+" AND "+schema.Ref(c.Config.ClickHouse.Database, shadow.Table))
				}
			}
		}
		if peer != nil {
			peer.Close()
		}
		if err != nil {
			return err
		}
	}
	published.Table = old.Table
	if err := c.Store.Own(published); err != nil {
		return err
	}
	old.Table = shadow.Table
	if err := c.Store.Own(old); err != nil {
		return err
	}
	return c.DropOwned(ctx, old)
}

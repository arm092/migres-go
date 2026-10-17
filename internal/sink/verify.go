package sink

import (
	"context"
	"fmt"

	"github.com/arm092/migres-go/internal/model"
)

func (c *ClickHouse) verifyGeneration(ctx context.Context, s model.Schema) error {
	if c.Config.ClickHouse.Engine != "ReplicatedReplacingMergeTree" {
		return nil
	}
	for _, endpoint := range c.Config.ClickHouse.Addresses {
		cfg := c.Config
		cfg.ClickHouse.Addresses = []string{endpoint}
		peer, err := Open(cfg)
		if err != nil {
			return err
		}
		var marker, engine string
		err = peer.Conn.QueryRow(ctx, "SELECT comment,engine FROM system.tables WHERE database=? AND name=?", c.Config.ClickHouse.Database, s.Table).Scan(&marker, &engine)
		peer.Close()
		if err != nil {
			return err
		}
		if marker != "migres:"+s.Generation || engine != c.Config.ClickHouse.Engine {
			return fmt.Errorf("replica %s table generation/engine mismatch", endpoint)
		}
	}
	return nil
}

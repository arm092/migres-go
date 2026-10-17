package sink

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/arm092/migres-go/internal/journal"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/schema"
)

func (c *ClickHouse) ArchiveTask(ctx context.Context, t model.Task) error {
	payload, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return c.archive(ctx, "tasks", t.ID, string(payload))
}
func (c *ClickHouse) archive(ctx context.Context, kind string, id uint64, payload string) error {
	c.archiveMu.Lock()
	defer c.archiveMu.Unlock()
	var instance string
	if err := c.Store.Meta("instance_id", &instance); err != nil {
		instance = journal.ID()
		if err = c.Store.SetMeta("instance_id", instance); err != nil {
			return err
		}
	}
	table := "_migres_debug_" + kind + "_" + instance
	cluster := ""
	if c.Config.ClickHouse.Engine == "ReplicatedReplacingMergeTree" {
		cluster = " ON CLUSTER " + schema.Quote(c.Config.ClickHouse.Cluster)
	}
	q := "CREATE TABLE IF NOT EXISTS " + schema.Ref(c.Config.ClickHouse.Database, table) + cluster + " (id UInt64, payload String, recorded DateTime DEFAULT now()) ENGINE=ReplacingMergeTree(recorded) ORDER BY id TTL recorded + INTERVAL " + fmt.Sprint(int64(c.Config.DebugRetention.Seconds())) + " SECOND DELETE COMMENT " + schema.Literal("migres:"+instance)
	var count uint64
	if err := c.Conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=? AND name=?", c.Config.ClickHouse.Database, table).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		var marker string
		if err := c.Conn.QueryRow(ctx, "SELECT comment FROM system.tables WHERE database=? AND name=?", c.Config.ClickHouse.Database, table).Scan(&marker); err != nil {
			return err
		}
		if marker != "migres:"+instance {
			return fmt.Errorf("debug archive table is not owned")
		}
	}
	if err := c.Conn.Exec(ctx, q); err != nil {
		return err
	}
	if err := c.Store.Own(model.Schema{Table: table, Generation: instance}); err != nil {
		return err
	}
	return c.Conn.Exec(ctx, "INSERT INTO "+schema.Ref(c.Config.ClickHouse.Database, table)+" (id,payload) VALUES (?,?)", id, payload)
}

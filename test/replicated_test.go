package test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/app"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/schema"
	"github.com/arm092/migres-go/internal/sink"
	"github.com/arm092/migres-go/internal/source"
)

func TestReplicatedQuorumAndRebuild(t *testing.T) {
	if os.Getenv("MIGRES_REPLICATED") != "1" {
		t.Skip("start test/replicated.yml and set MIGRES_REPLICATED=1")
	}
	c := testConfig(t)
	c.ClickHouse.Port = 39001
	c.ClickHouse.Addresses = []string{net.JoinHostPort(c.ClickHouse.Host, "39001"), net.JoinHostPort(c.ClickHouse.Host, "39002")}
	c.ClickHouse.Engine = "ReplicatedReplacingMergeTree"
	c.ClickHouse.Cluster = "analytics"
	name := fmt.Sprintf("replicated_%d", time.Now().UnixNano())
	c.MySQL.Include = []string{name}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	m, err := source.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	_, err = m.DB.ExecContext(ctx, "CREATE TABLE `"+name+"` (id BIGINT PRIMARY KEY,value INT NOT NULL)")
	if err != nil {
		t.Fatal(err)
	}
	defer m.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
	m.DB.ExecContext(ctx, "INSERT INTO `"+name+"` VALUES(1,42)")
	a, err := app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- a.Run(runCtx) }()
	defer func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	wait(t, ctx, func() bool { return a.Ready.Load() })
	if _, err = m.DB.ExecContext(ctx, "ALTER TABLE `"+name+"` MODIFY COLUMN value BIGINT NOT NULL"); err != nil {
		t.Fatal(err)
	}
	m.DB.ExecContext(ctx, "UPDATE `"+name+"` SET value=5000000001 WHERE id=1")
	for _, q := range []string{"ADD COLUMN extra INT NOT NULL DEFAULT 7", "RENAME COLUMN extra TO renamed", "DROP COLUMN renamed"} {
		if _, err = m.DB.ExecContext(ctx, "ALTER TABLE `"+name+"` "+q); err != nil {
			t.Fatal(err)
		}
	}
	for _, endpoint := range c.ClickHouse.Addresses {
		peerConfig := c
		peerConfig.ClickHouse.Addresses = []string{endpoint}
		peer, err := sink.Open(peerConfig)
		if err != nil {
			t.Fatal(err)
		}
		defer peer.Close()
		wait(t, ctx, func() bool {
			var n int64
			err := peer.Conn.QueryRow(ctx, "SELECT value FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&n)
			return err == nil && n == 5000000001
		})
	}
}

func TestReplicatedDropRecoveryAndOwnership(t *testing.T) {
	if os.Getenv("MIGRES_REPLICATED") != "1" {
		t.Skip("requires replicated fixture")
	}
	c := testConfig(t)
	c.ClickHouse.Addresses = []string{net.JoinHostPort(c.ClickHouse.Host, "39001"), net.JoinHostPort(c.ClickHouse.Host, "39002")}
	c.ClickHouse.Engine = "ReplicatedReplacingMergeTree"
	c.ClickHouse.Cluster = "analytics"
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	a, err := app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	s := model.Schema{Table: fmt.Sprintf("drop_%d", time.Now().UnixNano()), Generation: fmt.Sprintf("gen_%d", time.Now().UnixNano()), Columns: []model.Column{{Name: "id", CHType: "Int64"}}, Keys: []string{"id"}}
	if err = a.Sink.Ensure(ctx, s); err != nil {
		t.Fatal(err)
	}
	newName := s.Table + "_renamed"
	if err = a.Sink.Conn.Exec(ctx, "RENAME TABLE "+schema.Ref(c.ClickHouse.Database, s.Table)+" TO "+schema.Ref(c.ClickHouse.Database, newName)); err != nil {
		t.Fatal(err)
	}
	if err = a.Sink.Rename(ctx, [2]string{s.Table, newName}, " ON CLUSTER `analytics`"); err != nil {
		t.Fatal(err)
	}
	s.Table = newName
	pc := c
	pc.ClickHouse.Addresses = []string{c.ClickHouse.Addresses[1]}
	peer, err := sink.Open(pc)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	ref := schema.Ref(c.ClickHouse.Database, s.Table)
	if err = peer.Conn.Exec(ctx, "ALTER TABLE "+ref+" MODIFY COMMENT 'foreign'"); err != nil {
		t.Fatal(err)
	}
	if err = a.Sink.DropOwned(ctx, s); err == nil {
		t.Fatal("foreign replica generation was dropped")
	}
	var n uint64
	if err = a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=? AND name=?", c.ClickHouse.Database, s.Table).Scan(&n); err != nil || n != 1 {
		t.Fatalf("local table changed before ownership validation: %d %v", n, err)
	}
	if err = peer.Conn.Exec(ctx, "ALTER TABLE "+ref+" MODIFY COMMENT "+schema.Literal("migres:"+s.Generation)); err != nil {
		t.Fatal(err)
	}
	pc.ClickHouse.Addresses = []string{c.ClickHouse.Addresses[0]}
	first, err := sink.Open(pc)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if err = first.Conn.Exec(ctx, "DROP TABLE "+ref+" SYNC"); err != nil {
		t.Fatal(err)
	}
	if err = a.Sink.DropOwned(ctx, s); err != nil {
		t.Fatal(err)
	}
	if err = peer.Conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=? AND name=?", c.ClickHouse.Database, s.Table).Scan(&n); err != nil || n != 0 {
		t.Fatalf("remote table abandoned: %d %v", n, err)
	}
	owned, err := a.Store.Owned()
	if err != nil || len(owned) != 0 {
		t.Fatalf("ownership was not retired: %v %v", owned, err)
	}
}

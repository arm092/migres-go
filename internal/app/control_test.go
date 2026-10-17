package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/config"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/source"
)

func TestControlRepositionAndOwnedReset(t *testing.T) {
	if os.Getenv("MIGRES_INTEGRATION") != "1" {
		t.Skip("requires isolated test/compose.yml")
	}
	c := config.Default()
	if host := os.Getenv("MIGRES_TEST_HOST"); host != "" {
		c.MySQL.Host = host
		c.ClickHouse.Host = host
	}
	c.MySQL.Port = 33080
	c.MySQL.User = "root"
	c.MySQL.Password = "test-only"
	c.MySQL.Database = "migres_src"
	c.ClickHouse.Port = 39000
	c.ClickHouse.Password = "test-only"
	c.ClickHouse.Database = "migres_dst"
	c.DataDir = t.TempDir()
	c.RuntimeDir = filepath.Join(c.DataDir, "run")
	c.Listen = "127.0.0.1:0"
	c.ShutdownTimeout = 5 * time.Second
	c.Migration.CDC.UseGTID = true
	c.Migration.CDC.ServerID = 54379
	name := fmt.Sprintf("control_%d", time.Now().UnixNano())
	c.MySQL.Include = []string{name}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	src, err := source.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	src.DB.ExecContext(ctx, "CREATE TABLE `"+name+"` (id BIGINT PRIMARY KEY,value INT NOT NULL)")
	defer src.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
	src.DB.ExecContext(ctx, "INSERT INTO `"+name+"` VALUES(1,10)")
	a, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- a.Serve(runCtx) }()
	defer func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	until := func(fn func() bool) {
		t.Helper()
		for !fn() {
			select {
			case <-ctx.Done():
				t.Fatal("deadline")
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	until(func() bool { return a.Ready.Load() })
	var baseline model.Position
	a.Store.Meta("checkpoint", &baseline)
	src.DB.ExecContext(ctx, "UPDATE `"+name+"` SET value=20 WHERE id=1")
	until(func() bool {
		var n int32
		err := a.Sink.Conn.QueryRow(ctx, "SELECT value FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&n)
		return err == nil && n == 20
	})
	req := Request{Command: "reposition", File: baseline.File, Pos: baseline.Pos, ID: "replay-one"}
	response, err := Control(ctx, c, req)
	if err != nil {
		t.Fatal(err)
	}
	if response.Operation.State != "done" {
		t.Fatal(response)
	}
	until(func() bool { return a.Ready.Load() })
	repeat, err := Control(ctx, c, req)
	if err != nil || repeat.Operation.ID != "replay-one" {
		t.Fatalf("idempotency: %+v %v", repeat, err)
	}
	foreign := name + "_foreign"
	response, err = Control(ctx, c, Request{Command: "resnapshot", Table: name, ID: "copy-one"})
	if err != nil || response.Operation.State != "queued" {
		t.Fatalf("resnapshot: %+v %v", response, err)
	}
	until(func() bool {
		out, e := Control(ctx, c, Request{Command: "status", ID: "copy-one"})
		return e == nil && out.Operation != nil && out.Operation.State == "done" && a.Ready.Load()
	})
	a.Sink.Conn.Exec(ctx, "CREATE TABLE `"+foreign+"` (id Int64) ENGINE=MergeTree ORDER BY id")
	defer a.Sink.Conn.Exec(context.Background(), "DROP TABLE IF EXISTS `"+foreign+"`")
	response, err = Control(ctx, c, Request{Command: "reset", ID: "reset-one"})
	if err != nil {
		t.Fatal(err)
	}
	until(func() bool { return a.Ready.Load() })
	var count uint64
	if err = a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=? AND name=?", c.ClickHouse.Database, foreign).Scan(&count); err != nil || count != 1 {
		t.Fatal("reset touched unrelated table")
	}
}

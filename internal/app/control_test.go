package app

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/config"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/schema"
	"github.com/arm092/migres-go/internal/source"
)

func TestControl(t *testing.T) {
	for _, useGTID := range []bool{false, true} {
		t.Run(fmt.Sprintf("gtid_reader_%t", useGTID), func(t *testing.T) { testControlRepositionAndOwnedReset(t, useGTID) })
	}
}
func testControlRepositionAndOwnedReset(t *testing.T, useGTID bool) {
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
	c.Migration.CDC.UseGTID = useGTID
	c.Migration.CDC.ServerID = 100000 + rand.Uint32()%3000000000
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
	response, err = Control(ctx, c, Request{Command: "reposition", GTID: baseline.GTID, ID: "gtid-replay-one"})
	if err != nil || response.Operation.State != "done" {
		t.Fatalf("GTID command: %+v %v", response, err)
	}
	until(func() bool {
		var n int32
		e := a.Sink.Conn.QueryRow(ctx, "SELECT value FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&n)
		return a.Ready.Load() && e == nil && n == 20
	})
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

func TestDrainRebuild(t *testing.T) {
	if os.Getenv("MIGRES_INTEGRATION") != "1" {
		t.Skip("isolated fixture required")
	}
	c := config.Default()
	c.MySQL.Host = os.Getenv("MIGRES_TEST_HOST")
	if c.MySQL.Host == "" {
		c.MySQL.Host = "127.0.0.1"
	}
	c.ClickHouse.Host = c.MySQL.Host
	c.MySQL.Port = 33080
	c.MySQL.User = "root"
	c.MySQL.Password = "test-only"
	c.MySQL.Database = "migres_src"
	c.ClickHouse.Port = 39000
	c.ClickHouse.Password = "test-only"
	c.ClickHouse.Database = "migres_dst"
	c.DataDir = t.TempDir()
	c.ShutdownTimeout = 3 * time.Second
	c.Migration.SnapshotMode = "signal"
	name := fmt.Sprintf("drain_%d", time.Now().UnixNano())
	c.MySQL.Include = []string{name}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	a, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	exec := func(q string) {
		t.Helper()
		if _, e := a.Source.DB.ExecContext(ctx, q); e != nil {
			t.Fatal(e)
		}
	}
	exec("CREATE TABLE IF NOT EXISTS _migres_signal(id VARCHAR(64) PRIMARY KEY,marker VARCHAR(64) NOT NULL)")
	exec("CREATE TABLE `" + name + "`(id INT PRIMARY KEY,value INT NOT NULL)")
	defer a.Source.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
	exec("INSERT INTO `" + name + "` VALUES(1,42)")
	if err = a.Check(ctx); err != nil {
		t.Fatal(err)
	}
	if err = a.bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		owned, _ := a.Store.Owned()
		for _, s := range owned {
			a.Sink.DropOwned(context.Background(), s)
		}
	}()
	old, err := a.Store.Schema(name)
	if err != nil {
		t.Fatal(err)
	}
	query := "ALTER TABLE `" + name + "` MODIFY value BIGINT NOT NULL"
	exec(query)
	next, rebuild, err := schema.Apply(old, query, c)
	if err != nil || !rebuild {
		t.Fatal(err)
	}
	pos, err := a.Source.Position(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Store.SchemaEvent(model.Event{Kind: "ddl", Table: name, Schema: &next, Previous: &old, Rebuild: true, Query: query, Position: pos, SourceID: fmt.Sprintf("%s:%d", pos.File, pos.Pos)}, []model.Schema{next}, nil); err != nil {
		t.Fatal(err)
	}
	if err = a.drain(ctx); err != nil {
		t.Fatal(err)
	}
	var value int64
	if err = a.Sink.Conn.QueryRow(ctx, "SELECT value FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&value); err != nil || value != 42 {
		t.Fatalf("%d %v", value, err)
	}
}

func TestResetRecovery(t *testing.T) {
	for _, afterReset := range []bool{false, true} {
		t.Run(fmt.Sprint(afterReset), func(t *testing.T) {
			if os.Getenv("MIGRES_INTEGRATION") != "1" {
				t.Skip("isolated fixture required")
			}
			c := config.Default()
			c.MySQL.Host = os.Getenv("MIGRES_TEST_HOST")
			if c.MySQL.Host == "" {
				c.MySQL.Host = "127.0.0.1"
			}
			c.ClickHouse.Host = c.MySQL.Host
			c.MySQL.Port = 33080
			c.MySQL.User = "root"
			c.MySQL.Password = "test-only"
			c.MySQL.Database = "migres_src"
			c.ClickHouse.Port = 39000
			c.ClickHouse.Password = "test-only"
			c.ClickHouse.Database = "migres_dst"
			c.DataDir = t.TempDir()
			name := fmt.Sprintf("resetcrash_%d", time.Now().UnixNano())
			c.MySQL.Include = []string{name, name + "b"}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			a, err := Open(c)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { a.Close() }()
			for _, n := range c.MySQL.Include {
				if _, err = a.Source.DB.ExecContext(ctx, "CREATE TABLE `"+n+"`(id INT PRIMARY KEY)"); err != nil {
					t.Fatal(err)
				}
				defer func(table string) { a.Source.DB.Exec("DROP TABLE IF EXISTS `" + table + "`") }(n)
			}
			foreign := name + "foreign"
			if err = a.Sink.Conn.Exec(ctx, "CREATE TABLE `"+foreign+"`(id Int32) ENGINE=MergeTree ORDER BY id"); err != nil {
				t.Fatal(err)
			}
			defer func() { a.Sink.Conn.Exec(context.Background(), "DROP TABLE IF EXISTS `"+foreign+"`") }()
			if err = a.Check(ctx); err != nil {
				t.Fatal(err)
			}
			if err = a.bootstrap(ctx); err != nil {
				t.Fatal(err)
			}
			if err = a.Store.Operation(model.Operation{ID: "reset-recover", Kind: "reset", State: "applying"}); err != nil {
				t.Fatal(err)
			}
			owned, err := a.Store.Owned()
			if err != nil || len(owned) != 2 {
				t.Fatalf("%v %v", owned, err)
			}
			if err = a.Sink.DropOwned(ctx, owned[0]); err != nil {
				t.Fatal(err)
			}
			if afterReset {
				if err = a.applyRequest(ctx, Request{ID: "reset-recover", Command: "reset"}); err != nil {
					t.Fatal(err)
				}
			}
			a.Close() // reopen exactly at a durable crash boundary; no in-memory progress survives
			a, err = Open(c)
			if err != nil {
				t.Fatal(err)
			}
			if err = a.recoverOperations(ctx); err != nil {
				t.Fatal(err)
			}
			ops, err := a.Store.Operations()
			if err != nil || len(ops) != 1 || ops[0].State != "done" {
				t.Fatalf("%+v %v", ops, err)
			}
			owned, err = a.Store.Owned()
			if err != nil || len(owned) != 0 {
				t.Fatalf("orphan ownership %+v %v", owned, err)
			}
			var count uint64
			if err = a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=? AND name=?", c.ClickHouse.Database, foreign).Scan(&count); err != nil || count != 1 {
				t.Fatal("foreign table lost", err)
			}
		})
	}
}

func TestLostLog(t *testing.T) {
	if os.Getenv("MIGRES_SOURCE_FAULTS") != "1" {
		t.Skip("opt-in: purges ONLY isolated fixture binlogs")
	}
	c := config.Default()
	c.MySQL.Host = os.Getenv("MIGRES_TEST_HOST")
	if c.MySQL.Host == "" {
		c.MySQL.Host = "127.0.0.1"
	}
	c.ClickHouse.Host = c.MySQL.Host
	c.MySQL.Port = 33080
	c.MySQL.User = "root"
	c.MySQL.Password = "test-only"
	c.MySQL.Database = "migres_src"
	c.ClickHouse.Port = 39000
	c.ClickHouse.Password = "test-only"
	c.ClickHouse.Database = "migres_dst"
	c.DataDir = t.TempDir()
	c.RuntimeDir = c.DataDir + "/run"
	c.Listen = "127.0.0.1:0"
	name := fmt.Sprintf("lostlog_%d", time.Now().UnixNano())
	c.MySQL.Include = []string{name}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	a, err := Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err = a.Source.DB.ExecContext(ctx, "CREATE TABLE `"+name+"`(id INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	defer a.Source.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
	if _, err = a.Source.DB.ExecContext(ctx, "INSERT INTO `"+name+"` VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	if err = a.Check(ctx); err != nil {
		t.Fatal(err)
	}
	if err = a.bootstrap(ctx); err != nil {
		t.Fatal(err)
	}
	var before model.Position
	if err = a.Store.Meta("checkpoint", &before); err != nil {
		t.Fatal(err)
	}
	if _, err = a.Source.DB.ExecContext(ctx, "FLUSH BINARY LOGS"); err != nil {
		t.Fatal(err)
	}
	current, err := a.Source.Position(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.Source.DB.ExecContext(ctx, "PURGE BINARY LOGS TO "+schema.Literal(current.File)); err != nil {
		t.Fatal(err)
	}
	run, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- a.Serve(run) }()
	defer func() {
		stop()
		if e := <-done; e != nil {
			t.Error(e)
		}
	}()
	for !a.Blocked.Load() {
		select {
		case <-ctx.Done():
			t.Fatal("lost binlog did not block")
		case <-time.After(20 * time.Millisecond):
		}
	}
	var after model.Position
	if err = a.Store.Meta("checkpoint", &after); err != nil || after.File != before.File || after.Pos != before.Pos {
		t.Fatal("lost log advanced checkpoint", err)
	}
	response, err := Control(ctx, c, Request{Command: "reset", ID: "lost-log-reset"})
	if err != nil || response.Operation.State != "done" {
		t.Fatalf("%+v %v", response, err)
	}
	for !a.Ready.Load() {
		select {
		case <-ctx.Done():
			t.Fatal("resnapshot did not recover")
		case <-time.After(20 * time.Millisecond):
		}
	}
	var count uint64
	if err = a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&count); err != nil || count != 1 {
		t.Fatalf("%d %v", count, err)
	}
}

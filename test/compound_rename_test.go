package test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/arm092/migres-go/internal/app"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/schema"
	"github.com/arm092/migres-go/internal/sink"
	"github.com/arm092/migres-go/internal/source"
)

// The server commits a DDL statement, but its acknowledgement never reaches us.
type compoundLostResponse struct {
	driver.Conn
	match string
	fired bool
}

func (c *compoundLostResponse) Exec(ctx context.Context, query string, args ...any) error {
	err := c.Conn.Exec(ctx, query, args...)
	if err == nil && !c.fired && strings.Contains(query, c.match) {
		c.fired = true
		return context.DeadlineExceeded
	}
	return err
}

func TestCompoundTableRenameLostResponse(t *testing.T) {
	for _, boundary := range []string{"RENAME TABLE", "ADD COLUMN"} {
		t.Run(boundary, func(t *testing.T) {
			c := testConfig(t)
			if os.Getenv("MIGRES_REPLICATED") == "1" {
				c.ClickHouse.Addresses = []string{net.JoinHostPort(c.ClickHouse.Host, "39001"), net.JoinHostPort(c.ClickHouse.Host, "39002")}
				c.ClickHouse.Engine = "ReplicatedReplacingMergeTree"
				c.ClickHouse.Cluster = "analytics"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			a, err := app.Open(c)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				owned, _ := a.Store.Owned()
				for _, s := range owned {
					if e := a.Sink.DropOwned(context.Background(), s); e != nil {
						t.Error(e)
					}
				}
				a.Close()
			}()
			old := model.Schema{Table: fmt.Sprintf("compound_ack_%d", time.Now().UnixNano()), Generation: fmt.Sprintf("gen_%d", time.Now().UnixNano()), Columns: []model.Column{{Name: "id", MySQLType: "bigint", CHType: "Int64"}, {Name: "value", MySQLType: "int", CHType: "Int32"}, {Name: "obsolete", MySQLType: "int", CHType: "Int32"}}, Keys: []string{"id"}}
			if err = a.Sink.Ensure(ctx, old); err != nil {
				t.Fatal(err)
			}
			if err = a.Sink.Execute(ctx, model.Task{Kind: "insert", Table: old.Table, Schema: &old, Token: old.Generation + ":row", Rows: []model.Row{{Values: []model.Value{model.Encode(int64(1)), model.Encode(int32(73)), model.Encode(int32(9))}, Version: 2}}}); err != nil {
				t.Fatal(err)
			}
			var keeperPath string
			if c.ClickHouse.Engine == "ReplicatedReplacingMergeTree" {
				if err = a.Sink.Conn.QueryRow(ctx, "SELECT zookeeper_path FROM system.replicas WHERE database=? AND table=?", c.ClickHouse.Database, old.Table).Scan(&keeperPath); err != nil {
					t.Fatal(err)
				}
			}
			query := "ALTER TABLE `" + old.Table + "` CHANGE COLUMN value preserved INT NOT NULL, ADD COLUMN value INT NOT NULL DEFAULT 4, DROP COLUMN obsolete, RENAME TO `" + old.Table + "_new`"
			fresh, rebuild, err := schema.Apply(old, query, c)
			if err != nil || rebuild {
				t.Fatal(rebuild, err)
			}
			task := model.Task{Kind: "ddl", Table: old.Table, Previous: &old, Schema: &fresh, SQL: query, Token: old.Generation + ":compound"}
			if err = a.Store.SetMeta("snapshot_done", true); err != nil {
				t.Fatal(err)
			}
			injected := &compoundLostResponse{Conn: a.Sink.Conn, match: boundary}
			a.Sink.Conn = injected
			if err = a.Execute(ctx, task); !errors.Is(err, context.DeadlineExceeded) || !injected.fired {
				t.Fatalf("missing injected response loss: fired=%v err=%v", injected.fired, err)
			}
			a.Close()
			a, err = app.Open(c)
			if err != nil {
				t.Fatal(err)
			}
			for attempt := 0; attempt < 2; attempt++ {
				if err = a.Execute(ctx, task); err != nil {
					t.Fatal(err)
				}
			}
			endpoints := c.ClickHouse.Addresses
			if len(endpoints) == 0 {
				endpoints = []string{""}
			}
			for _, endpoint := range endpoints {
				conn := a.Sink.Conn
				if endpoint != "" {
					cfg := c
					cfg.ClickHouse.Addresses = []string{endpoint}
					peer, e := sink.Open(cfg)
					if e != nil {
						t.Fatal(e)
					}
					defer peer.Close()
					conn = peer.Conn
				}
				var marker string
				if err = conn.QueryRow(ctx, "SELECT comment FROM system.tables WHERE database=? AND name=?", c.ClickHouse.Database, fresh.Table).Scan(&marker); err != nil || marker != "migres:"+old.Generation {
					t.Fatalf("generation changed: %q %v", marker, err)
				}
				if keeperPath != "" {
					var actualPath string
					if err = conn.QueryRow(ctx, "SELECT zookeeper_path FROM system.replicas WHERE database=? AND table=?", c.ClickHouse.Database, fresh.Table).Scan(&actualPath); err != nil || actualPath != keeperPath {
						t.Fatalf("Keeper path changed: %q -> %q %v", keeperPath, actualPath, err)
					}
				}
				var preserved, value int32
				if err = conn.QueryRow(ctx, "SELECT preserved,value FROM "+schema.Ref(c.ClickHouse.Database, fresh.Table)+" FINAL WHERE _migres_deleted=0").Scan(&preserved, &value); err != nil || preserved != 73 || value != 4 {
					t.Fatalf("replay corrupted reused column: %d %d %v", preserved, value, err)
				}
			}
		})
	}
}

func TestCompoundTableRenameCDC(t *testing.T) {
	for _, mode := range []string{"compatible", "rebuild", "backlog"} {
		t.Run(mode, func(t *testing.T) {
			c := testConfig(t)
			old := fmt.Sprintf("compound_move_%d", time.Now().UnixNano())
			moved, last := old+"_moved", old+"_last"
			c.MySQL.Include = []string{old}
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			m, err := source.Open(c)
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			exec := func(q string) {
				t.Helper()
				if _, e := m.DB.ExecContext(ctx, q); e != nil {
					t.Fatal(e)
				}
			}
			exec("CREATE TABLE `" + old + "` (id INT PRIMARY KEY,value INT NOT NULL,obsolete INT NOT NULL)")
			defer m.DB.Exec("DROP TABLE IF EXISTS `" + old + "`,`" + moved + "`,`" + last + "`")
			exec("INSERT INTO `" + old + "` VALUES(1,73,9)")
			a, err := app.Open(c)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				owned, _ := a.Store.Owned()
				for _, s := range owned {
					a.Sink.DropOwned(context.Background(), s)
				}
				a.Close()
			}()
			var stop context.CancelFunc
			var done chan error
			start := func() {
				run, halt := context.WithCancel(ctx)
				stop = halt
				done = make(chan error, 1)
				go func() { done <- a.Run(run) }()
				wait(t, ctx, func() bool { return a.Ready.Load() })
			}
			finish := func() {
				stop()
				stop = nil
				if e := <-done; e != nil {
					t.Fatal(e)
				}
			}
			start()
			defer func() {
				if stop != nil {
					stop()
					<-done
				}
			}()
			wait(t, ctx, func() bool {
				var n uint64
				e := a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM `"+old+"` FINAL WHERE _migres_deleted=0").Scan(&n)
				return e == nil && n == 1
			})
			if mode == "backlog" {
				finish()
				a.Close()
				exec("ALTER TABLE `" + old + "` MODIFY COLUMN value BIGINT NOT NULL")
			}
			typ := "INT"
			if mode == "backlog" {
				typ = "BIGINT"
			}
			if mode == "rebuild" {
				typ = "VARCHAR(20)"
			}
			exec("ALTER TABLE `" + old + "` CHANGE COLUMN value preserved " + typ + " NOT NULL, ADD COLUMN value INT NOT NULL DEFAULT 4, DROP COLUMN obsolete, RENAME TO `" + moved + "`")
			exec("UPDATE `" + moved + "` SET value=8 WHERE id=1")
			if mode == "backlog" {
				a, err = app.Open(c)
				if err != nil {
					t.Fatal(err)
				}
				start()
			}
			check := func(table string) {
				t.Helper()
				wait(t, ctx, func() bool {
					var p string
					var v int32
					e := a.Sink.Conn.QueryRow(ctx, "SELECT toString(preserved),value FROM `"+table+"` FINAL WHERE id=1 AND _migres_deleted=0").Scan(&p, &v)
					return e == nil && p == "73" && v == 8
				})
			}
			check(moved)
			finish()
			stop = nil
			a.Close()
			a, err = app.Open(c)
			if err != nil {
				t.Fatal(err)
			}
			start()
			exec("ALTER TABLE `" + moved + "` RENAME TO `" + last + "`")
			check(last)
			exec("CREATE TABLE `" + old + "` (id INT PRIMARY KEY,value INT NOT NULL)")
			exec("INSERT INTO `" + old + "` VALUES(2,99)")
			wait(t, ctx, func() bool {
				var v int32
				e := a.Sink.Conn.QueryRow(ctx, "SELECT value FROM `"+old+"` FINAL WHERE id=2 AND _migres_deleted=0").Scan(&v)
				return e == nil && v == 99
			})
			exec("DROP TABLE `" + last + "`")
			wait(t, ctx, func() bool {
				var n uint64
				e := a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=currentDatabase() AND name=?", last).Scan(&n)
				return e == nil && n == 0
			})
			finish()
			stop = nil
		})
	}
}

type compoundSnapshotGate struct {
	driver.Conn
	entered, release chan struct{}
	once             sync.Once
}

func (g *compoundSnapshotGate) PrepareBatch(ctx context.Context, q string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	g.once.Do(func() {
		close(g.entered)
		select {
		case <-g.release:
		case <-ctx.Done():
		}
	})
	return g.Conn.PrepareBatch(ctx, q, opts...)
}

func TestCompoundTableRenameDuringSnapshot(t *testing.T) {
	c := testConfig(t)
	c.Migration.BatchRows = 1
	name := fmt.Sprintf("compound_snap_%d", time.Now().UnixNano())
	moved := name + "_new"
	c.MySQL.Include = []string{name}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	m, err := source.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	exec := func(q string) {
		t.Helper()
		if _, e := m.DB.ExecContext(ctx, q); e != nil {
			t.Fatal(e)
		}
	}
	exec("CREATE TABLE `" + name + "` (id INT PRIMARY KEY,value INT NOT NULL)")
	defer m.DB.Exec("DROP TABLE IF EXISTS `" + name + "`,`" + moved + "`")
	exec("INSERT INTO `" + name + "` VALUES (1,10),(2,20),(3,30)")
	a, err := app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	defer func() {
		owned, _ := a.Store.Owned()
		for _, s := range owned {
			a.Sink.DropOwned(context.Background(), s)
		}
	}()
	gate := &compoundSnapshotGate{Conn: a.Sink.Conn, entered: make(chan struct{}), release: make(chan struct{})}
	a.Sink.Conn = gate
	run, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- a.Run(run) }()
	released := false
	defer func() {
		if !released {
			close(gate.release)
		}
		stop()
		if e := <-done; e != nil {
			t.Error(e)
		}
	}()
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("snapshot INSERT never reached gate")
	}
	if a.Ready.Load() {
		t.Fatal("snapshot unexpectedly completed")
	}
	_, err = a.Store.Schema(name)
	if err != nil {
		t.Fatal(err)
	}
	exec("ALTER TABLE `" + name + "` ADD COLUMN extra INT NOT NULL DEFAULT 7, RENAME TO `" + moved + "`")
	// Wait for durable producer metadata while the initial snapshot is still held.
	wait(t, ctx, func() bool { s, e := a.Store.Schema(moved); return e == nil && s.Table == moved })
	close(gate.release)
	released = true
	wait(t, ctx, func() bool {
		var count uint64
		var sum int64
		e := a.Sink.Conn.QueryRow(ctx, "SELECT count(),sum(value) FROM `"+moved+"` FINAL WHERE _migres_deleted=0 AND extra=7").Scan(&count, &sum)
		return e == nil && count == 3 && sum == 60
	})
	wait(t, ctx, func() bool { return a.Ready.Load() })
	var oldCount uint64
	if err = a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=currentDatabase() AND name=?", name).Scan(&oldCount); err != nil || oldCount != 0 {
		t.Fatalf("old snapshot table remains: %d %v", oldCount, err)
	}
}

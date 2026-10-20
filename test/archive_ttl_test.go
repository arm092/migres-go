package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/app"
	"github.com/arm092/migres-go/internal/journal"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/schema"
	"github.com/arm092/migres-go/internal/source"
)

// This is an opt-in real-server check: TTL is observed through ordinary reads,
// never OPTIMIZE FINAL or MATERIALIZE TTL. Only fixture-owned tables are changed.
func TestDebugArchiveTTL(t *testing.T) {
	for _, debug := range []bool{false, true} {
		t.Run(fmt.Sprintf("debug_%t", debug), func(t *testing.T) {
			c := testConfig(t)
			c.Migration.CDC.DBDebug = debug
			c.DebugRetention = 12 * time.Second
			name := fmt.Sprintf("archive_ttl_%d", time.Now().UnixNano())
			c.MySQL.Include = []string{name}
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			m, err := source.Open(c)
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			if _, err = m.DB.ExecContext(ctx, "CREATE TABLE `"+name+"` (id INT PRIMARY KEY, value INT NOT NULL)"); err != nil {
				t.Fatal(err)
			}
			defer m.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
			if _, err = m.DB.ExecContext(ctx, "INSERT INTO `"+name+"` VALUES (1,10)"); err != nil {
				t.Fatal(err)
			}
			a, err := app.Open(c)
			if err != nil {
				t.Fatal(err)
			}
			if err = a.Store.SetMeta("instance_id", journal.ID()); err != nil {
				t.Fatal(err)
			}
			defer func() { a.Close() }()
			defer func() {
				owned, e := a.Store.Owned()
				if e != nil {
					t.Error(e)
					return
				}
				for _, s := range owned {
					if e = a.Sink.DropOwned(context.Background(), s); e != nil {
						t.Error(e)
					}
				}
			}()
			runCtx, stop := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- a.Run(runCtx) }()
			running := true
			defer func() {
				if running {
					stop()
					if e := <-done; e != nil {
						t.Error(e)
					}
				}
			}()
			wait(t, ctx, func() bool {
				if a.Blocked.Load() {
					var failure string
					a.Store.Meta("last_error", &failure)
					t.Fatal(failure)
				}
				return a.Ready.Load()
			})
			if _, err = m.DB.ExecContext(ctx, "UPDATE `"+name+"` SET value=20 WHERE id=1"); err != nil {
				t.Fatal(err)
			}
			wait(t, ctx, func() bool {
				var v int32
				return a.Sink.Conn.QueryRow(ctx, "SELECT value FROM `"+name+"` FINAL WHERE id=1 AND _migres_deleted=0 LIMIT 1 SETTINGS max_execution_time=5,max_rows_to_read=1000").Scan(&v) == nil && v == 20
			})
			stop()
			if err = <-done; err != nil {
				t.Fatal(err)
			}
			running = false
			var instance string
			if err = a.Store.Meta("instance_id", &instance); err != nil {
				t.Fatal(err)
			}
			tables := []string{"_migres_debug_events_" + instance, "_migres_debug_tasks_" + instance}
			if !debug {
				for _, table := range tables {
					var n uint64
					if err = a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=? AND name=? LIMIT 1 SETTINGS max_execution_time=5,max_rows_to_read=10000", c.ClickHouse.Database, table).Scan(&n); err != nil || n != 0 {
						t.Fatalf("normal mode archive %s count=%d err=%v", table, n, err)
					}
				}
				return
			}
			for _, table := range tables {
				var ddl, marker string
				if err = a.Sink.Conn.QueryRow(ctx, "SELECT create_table_query,comment FROM system.tables WHERE database=? AND name=? LIMIT 1 SETTINGS max_execution_time=5,max_rows_to_read=10000", c.ClickHouse.Database, table).Scan(&ddl, &marker); err != nil {
					t.Fatal(err)
				}
				if marker != "migres:"+instance || !strings.Contains(ddl, "toIntervalSecond(12)") {
					t.Fatalf("unexpected ownership/TTL %s %s", marker, ddl)
				}
				t.Logf("owned archive %s DDL=%s", table, ddl)
				if err = a.Sink.Conn.Exec(ctx, "ALTER TABLE "+schema.Ref(c.ClickHouse.Database, table)+" MODIFY SETTING merge_with_ttl_timeout=1"); err != nil {
					t.Fatal(err)
				}
			}
			write := func(id uint64) {
				t.Helper()
				if err := a.Sink.Execute(ctx, model.Task{Kind: "archive", Payload: `{"ttl_probe":true}`, Rows: []model.Row{{Version: id}}}); err != nil {
					t.Fatal(err)
				}
				if err := a.Sink.ArchiveTask(ctx, model.Task{ID: id, Kind: "ttl_probe"}); err != nil {
					t.Fatal(err)
				}
			}
			count := func(table string, id uint64) uint64 {
				t.Helper()
				var n uint64
				if err := a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM "+schema.Ref(c.ClickHouse.Database, table)+" WHERE id=? LIMIT 1 SETTINGS max_execution_time=5,max_rows_to_read=10000", id).Scan(&n); err != nil {
					t.Fatal(err)
				}
				return n
			}
			const oldID uint64 = 9000000000000000001
			const freshID uint64 = 9000000000000000002
			write(oldID)
			started := time.Now()
			for _, table := range tables {
				if count(table, oldID) != 1 {
					t.Fatal("old probe missing immediately after insert")
				}
			}
			time.Sleep(9 * time.Second)
			write(freshID)
			wait(t, ctx, func() bool {
				for _, table := range tables {
					if count(table, oldID) != 0 {
						return false
					}
				}
				return true
			})
			for _, table := range tables {
				if count(table, freshID) != 1 {
					t.Fatalf("fresh probe expired early: %s", table)
				}
			}
			t.Logf("natural background TTL removed old probes and retained fresh probes after %s", time.Since(started))
			a.Close()
			c.DebugRetention = 30 * time.Second
			a, err = app.Open(c)
			if err != nil {
				t.Fatal(err)
			}
			write(freshID + 1)
			for _, table := range tables {
				var ddl string
				if err = a.Sink.Conn.QueryRow(ctx, "SHOW CREATE TABLE "+schema.Ref(c.ClickHouse.Database, table)).Scan(&ddl); err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(ddl, "toIntervalSecond(30)") {
					t.Fatalf("restart ignored new debug retention: %s", ddl)
				}
			}
		})
	}
}

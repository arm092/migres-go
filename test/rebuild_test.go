package test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/app"
	"github.com/arm092/migres-go/internal/source"
)

func TestTypeChangeRebuild(t *testing.T) {
	c := testConfig(t)
	name := fmt.Sprintf("rebuild_%d", time.Now().UnixNano())
	c.MySQL.Include = []string{name}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
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
	if _, err = m.DB.ExecContext(ctx, "UPDATE `"+name+"` SET value=5000000000 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	wait(t, ctx, func() bool {
		var n int64
		err := a.Sink.Conn.QueryRow(ctx, "SELECT value FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&n)
		return err == nil && n == 5000000000
	})
}

func TestPrimaryKeyChangeRebuild(t *testing.T) {
	c := testConfig(t)
	name := fmt.Sprintf("key_rebuild_%d", time.Now().UnixNano())
	c.MySQL.Include = []string{name}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	m, err := source.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	exec := func(q string) {
		t.Helper()
		if _, err := m.DB.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE TABLE `" + name + "`(id BIGINT PRIMARY KEY,replacement BIGINT,value INT NOT NULL)")
	defer m.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
	exec("INSERT INTO `" + name + "` VALUES(1,100,10),(2,200,20)")
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
	exec("ALTER TABLE `" + name + "` DROP PRIMARY KEY, ADD PRIMARY KEY(replacement)")
	exec("UPDATE `" + name + "` SET replacement=300,value=99 WHERE id=2")
	wait(t, ctx, func() bool {
		var n uint64
		var value int32
		var key string
		e := a.Sink.Conn.QueryRow(ctx, "SELECT count(),max(value) FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&n, &value)
		if e != nil || n != 2 || value != 99 {
			return false
		}
		e = a.Sink.Conn.QueryRow(ctx, "SELECT sorting_key FROM system.tables WHERE database=? AND name=?", c.ClickHouse.Database, name).Scan(&key)
		if e != nil || strings.Trim(key, "() ") != "replacement" {
			return false
		}
		e = a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM `"+name+"` FINAL WHERE _migres_deleted=0 AND replacement=200").Scan(&n)
		return e == nil && n == 0
	})
}

func TestRebuildCapacityRecovery(t *testing.T) {
	c := testConfig(t)
	name := fmt.Sprintf("capacity_%d", time.Now().UnixNano())
	c.MySQL.Include = []string{name}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	m, err := source.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	exec := func(q string) {
		t.Helper()
		if _, err := m.DB.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE TABLE `" + name + "`(id BIGINT PRIMARY KEY,value INT NOT NULL)")
	defer m.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
	exec("INSERT INTO `" + name + "` VALUES(1,1)")
	func() {
		a, e := app.Open(c)
		if e != nil {
			t.Fatal(e)
		}
		defer a.Close()
		run, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- a.Run(run) }()
		defer func() {
			stop()
			if e := <-done; e != nil {
				t.Error(e)
			}
		}()
		wait(t, ctx, func() bool { return a.Ready.Load() })
	}()
	exec("ALTER TABLE `" + name + "` MODIFY COLUMN value BIGINT NOT NULL")
	exec("UPDATE `" + name + "` SET value=5000000000")
	c.MaxQueueBytes = 128
	func() {
		a, e := app.Open(c)
		if e != nil {
			t.Fatal(e)
		}
		defer a.Close()
		e = a.Run(ctx)
		if e == nil || !strings.Contains(e.Error(), "max_queue_bytes") {
			t.Fatalf("capacity did not fail closed: %v", e)
		}
		stats, e := a.Store.Stats()
		if e != nil || stats.Tasks == 0 {
			t.Fatalf("coordinator was lost: %+v %v", stats, e)
		}
	}()
	c.MaxQueueBytes = 10 << 30
	a, err := app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	run, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- a.Run(run) }()
	defer func() {
		stop()
		if e := <-done; e != nil {
			t.Error(e)
		}
	}()
	wait(t, ctx, func() bool {
		var value int64
		e := a.Sink.Conn.QueryRow(ctx, "SELECT value FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&value)
		return a.Ready.Load() && e == nil && value == 5000000000
	})
}

// A DDL backlog may move or drop a source before Consumer reaches its rebuild.
// Reopening must converge without publishing stale row tasks from old names.
func TestRebuildSupersedingDDL(t *testing.T) {
	for _, kind := range []string{"rename", "drop", "reuse", "multi_drop", "swap"} {
		t.Run(kind, func(t *testing.T) {
			c := testConfig(t)
			if os.Getenv("MIGRES_LINEAGE_REPLICATED") == "1" {
				c.ClickHouse.Engine = "ReplicatedReplacingMergeTree"
				c.ClickHouse.Cluster = "analytics"
				c.ClickHouse.Addresses = []string{net.JoinHostPort(c.ClickHouse.Host, "39001"), net.JoinHostPort(c.ClickHouse.Host, "39002")}
			}
			name := fmt.Sprintf("supersede_%d", time.Now().UnixNano())
			renamed := name + "_new"
			c.MySQL.Include = []string{name}
			if kind == "multi_drop" || kind == "swap" {
				c.MySQL.Include = append(c.MySQL.Include, name+"c")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
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
			exec("CREATE TABLE `" + name + "`(id BIGINT PRIMARY KEY,value INT NOT NULL)")
			defer m.DB.Exec("DROP TABLE IF EXISTS `" + name + "`,`" + renamed + "`")
			exec("INSERT INTO `" + name + "` VALUES(1,10)")
			func() {
				a, e := app.Open(c)
				if e != nil {
					t.Fatal(e)
				}
				defer a.Close()
				run, stop := context.WithCancel(ctx)
				done := make(chan error, 1)
				go func() { done <- a.Run(run) }()
				defer func() {
					stop()
					if e := <-done; e != nil {
						t.Error(e)
					}
				}()
				wait(t, ctx, func() bool { return a.Ready.Load() })
			}()
			exec("ALTER TABLE `" + name + "` MODIFY value BIGINT NOT NULL")
			exec("UPDATE `" + name + "` SET value=5000000000")
			if kind == "rename" || kind == "reuse" {
				exec("RENAME TABLE `" + name + "` TO `" + renamed + "`")
				exec("UPDATE `" + renamed + "` SET id=2,value=6000000000")
				if kind == "reuse" {
					exec("CREATE TABLE `" + name + "`(id BIGINT PRIMARY KEY,value BIGINT NOT NULL)")
					exec("INSERT INTO `" + name + "` VALUES(100,7)")
				}

			} else if kind == "swap" {
				extra := name + "c"
				temp := name + "tmp"
				exec("CREATE TABLE `" + extra + "`(id BIGINT PRIMARY KEY,value BIGINT NOT NULL)")
				exec("INSERT INTO `" + extra + "` VALUES(99,9)")
				defer m.DB.Exec("DROP TABLE IF EXISTS `" + extra + "`,`" + temp + "`")
				exec("RENAME TABLE `" + name + "` TO `" + temp + "`,`" + extra + "` TO `" + name + "`,`" + temp + "` TO `" + extra + "`")
			} else if kind == "multi_drop" {
				extra := name + "c"
				moved := name + "d"
				exec("CREATE TABLE `" + extra + "`(id INT PRIMARY KEY)")
				exec("INSERT INTO `" + extra + "` VALUES(7)")
				defer m.DB.Exec("DROP TABLE IF EXISTS `" + extra + "`,`" + moved + "`")
				exec("RENAME TABLE `" + name + "` TO `" + renamed + "`,`" + extra + "` TO `" + moved + "`")
				exec("DROP TABLE `" + renamed + "`")
			} else {
				exec("DROP TABLE `" + name + "`")
			}
			a, err := app.Open(c)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			run, stop := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- a.Run(run) }()
			defer func() {
				stop()
				if e := <-done; e != nil {
					t.Error(e)
				}
			}()
			wait(t, ctx, func() bool {
				stats, e := a.Store.Stats()
				if e != nil || stats.Tasks != 0 || stats.Events != 0 {
					return false
				}
				var count uint64
				if kind == "drop" || kind == "multi_drop" {
					e = a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=? AND name=?", c.ClickHouse.Database, name).Scan(&count)
					if e != nil || count != 0 {
						return false
					}
					if kind == "multi_drop" {
						e = a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM `"+name+"d` FINAL WHERE _migres_deleted=0 AND id=7").Scan(&count)
						return e == nil && count == 1
					}
					return true
				}
				var id, value int64
				if kind == "swap" {
					e = a.Sink.Conn.QueryRow(ctx, "SELECT count(),max(id),max(value) FROM `"+name+"c` FINAL WHERE _migres_deleted=0").Scan(&count, &id, &value)
					if e != nil || count != 1 || id != 1 || value != 5000000000 {
						return false
					}
					e = a.Sink.Conn.QueryRow(ctx, "SELECT count(),max(id),max(value) FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&count, &id, &value)
					return e == nil && count == 1 && id == 99 && value == 9
				}
				e = a.Sink.Conn.QueryRow(ctx, "SELECT count(),max(id),max(value) FROM `"+renamed+"` FINAL WHERE _migres_deleted=0").Scan(&count, &id, &value)
				if e != nil || count != 1 || id != 2 || value != 6000000000 {
					return false
				}
				if kind == "reuse" {
					e = a.Sink.Conn.QueryRow(ctx, "SELECT count(),max(id),max(value) FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&count, &id, &value)
					return e == nil && count == 1 && id == 100 && value == 7
				}
				return true
			})
		})
	}
}

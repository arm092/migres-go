package test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/app"
	"github.com/arm092/migres-go/internal/config"
	"github.com/arm092/migres-go/internal/source"
)

func testConfig(t *testing.T) config.Config {
	t.Helper()
	if os.Getenv("MIGRES_INTEGRATION") != "1" {
		t.Skip("set MIGRES_INTEGRATION=1 after starting test/compose.yml")
	}
	c := config.Default()
	c.Migration.CDC.ServerID = 100000 + rand.Uint32()%3000000000
	if host := os.Getenv("MIGRES_TEST_HOST"); host != "" {
		c.MySQL.Host = host
		c.ClickHouse.Host = host
	}
	c.MySQL.Port = 33080
	if raw := os.Getenv("MIGRES_MYSQL_PORT"); raw != "" {
		port, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatal(err)
		}
		c.MySQL.Port = port
	}
	c.MySQL.User = "root"
	c.MySQL.Password = "test-only"
	c.MySQL.Database = "migres_src"
	c.ClickHouse.Port = 39000
	if raw := os.Getenv("MIGRES_CLICKHOUSE_PORT"); raw != "" {
		port, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatal(err)
		}
		c.ClickHouse.Port = port
	}
	c.ClickHouse.Password = "test-only"
	c.ClickHouse.Database = "migres_dst"
	c.DataDir = t.TempDir()
	c.RuntimeDir = c.DataDir + "/run"
	c.Listen = "127.0.0.1:0"
	c.ShutdownTimeout = 5 * time.Second
	src, err := source.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	if _, err = src.DB.Exec("CREATE TABLE IF NOT EXISTS _migres_signal (id VARCHAR(64) PRIMARY KEY,marker VARCHAR(64) NOT NULL) ENGINE=InnoDB"); err != nil {
		t.Fatal(err)
	}
	return c
}
func wait(t *testing.T, ctx context.Context, fn func() bool) {
	t.Helper()
	for {
		if fn() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("condition not reached before timeout")
		case <-time.After(100 * time.Millisecond):
		}
	}
}
func TestSnapshotCDCAndRestart(t *testing.T) {
	c := testConfig(t)
	name := fmt.Sprintf("orders_%d", time.Now().UnixNano())
	c.MySQL.Include = []string{name}
	m, err := source.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx, deadline := context.WithTimeout(context.Background(), 60*time.Second)
	defer deadline()
	_, err = m.DB.ExecContext(ctx, "CREATE TABLE `"+name+"` (tenant BIGINT UNSIGNED NOT NULL, id BIGINT UNSIGNED NOT NULL, name VARCHAR(100), amount DECIMAL(30,8), PRIMARY KEY(tenant,id)) ENGINE=InnoDB")
	if err != nil {
		t.Fatal(err)
	}
	defer m.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
	if _, err = m.DB.ExecContext(ctx, "INSERT INTO `"+name+"` VALUES (1,1,'one',12345678901234567890.12345678),(2,1,'two',2.0)"); err != nil {
		t.Fatal(err)
	}
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
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("shutdown exceeded deadline")
		}
	}()
	wait(t, ctx, func() bool { return a.Ready.Load() })
	count := func() uint64 {
		var n uint64
		a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&n)
		return n
	}
	wait(t, ctx, func() bool { return count() == 2 })
	if _, err = m.DB.ExecContext(ctx, "UPDATE `"+name+"` SET id=3,name='updated' WHERE tenant=1 AND id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err = m.DB.ExecContext(ctx, "DELETE FROM `"+name+"` WHERE tenant=2 AND id=1"); err != nil {
		t.Fatal(err)
	}
	wait(t, ctx, func() bool {
		var id uint64
		var amount string
		err := a.Sink.Conn.QueryRow(ctx, "SELECT id,toString(amount) FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&id, &amount)
		return err == nil && id == 3 && amount == "12345678901234567890.12345678" && count() == 1
	})
	tx, err := m.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx.ExecContext(ctx, "INSERT INTO `"+name+"` VALUES (3,8,'rollback',8)")
	tx.Rollback()
	time.Sleep(300 * time.Millisecond)
	if count() != 1 {
		t.Fatal("rolled-back row replicated")
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	a.Close()
	if _, err = m.DB.ExecContext(ctx, "INSERT INTO `"+name+"` VALUES(9,9,'offline',9)"); err != nil {
		t.Fatal(err)
	}
	a, err = app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	runCtx, stop = context.WithCancel(ctx)
	defer stop()
	done = make(chan error, 1)
	go func() { done <- a.Run(runCtx) }()
	wait(t, ctx, func() bool { return a.Ready.Load() && count() == 2 })
	boundary, err := m.Position(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if boundary.GTID != "" {
		defer func() {
			if t.Failed() {
				known, err := a.Store.Boundary(boundary.File, boundary.Pos)
				stats, _ := a.Store.Stats()
				t.Logf("expected boundary=%+v stored=%+v err=%v checkpoint=%+v", boundary, known, err, stats.Checkpoint)
			}
		}()

		wait(t, ctx, func() bool {
			points, e := a.Store.Boundaries()
			if e != nil {
				return false
			}
			for _, p := range points {
				if p.File == boundary.File && p.Pos == boundary.Pos {
					return p.GTID == boundary.GTID
				}
			}
			return false
		})
	}
}

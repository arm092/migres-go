package test

import (
	"context"
	"fmt"
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
		if e != nil || key != "replacement" {
			return false
		}
		e = a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM `"+name+"` FINAL WHERE _migres_deleted=0 AND replacement=200").Scan(&n)
		return e == nil && n == 0
	})
}

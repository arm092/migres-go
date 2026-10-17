package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/app"
	"github.com/arm092/migres-go/internal/source"
)

func TestDDLAndHistoricalSchema(t *testing.T) {
	c := testConfig(t)
	base := fmt.Sprintf("ddl_%d", time.Now().UnixNano())
	other := base + "_new"
	renamed := base + "_renamed"
	c.MySQL.Include = []string{base, other}
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
	exec("CREATE TABLE `" + base + "` (id BIGINT PRIMARY KEY, value VARCHAR(50))")
	defer func() { m.DB.Exec("DROP TABLE IF EXISTS `" + base + "`, `" + other + "`, `" + renamed + "`") }()
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
	exec("ALTER TABLE `" + base + "` ADD COLUMN extra INT NOT NULL DEFAULT 4")
	exec("INSERT INTO `" + base + "` (id,value) VALUES (1,'42')")
	exec("ALTER TABLE `" + base + "` CHANGE COLUMN value label VARCHAR(50)")
	exec("UPDATE `" + base + "` SET label='43' WHERE id=1")
	wait(t, ctx, func() bool {
		var v string
		var n int32
		err := a.Sink.Conn.QueryRow(ctx, "SELECT label,extra FROM `"+base+"` FINAL WHERE _migres_deleted=0").Scan(&v, &n)
		return err == nil && v == "43" && n == 4
	})
	exec("ALTER TABLE `" + base + "` DROP COLUMN extra")
	exec("CREATE TABLE `" + other + "` (id BIGINT PRIMARY KEY, label VARCHAR(20))")
	exec("INSERT INTO `" + other + "` VALUES(1,'created')")
	exec("ALTER TABLE `" + c.MySQL.Database + "`.`" + other + "` RENAME TO `" + renamed + "`")
	exec("UPDATE `" + renamed + "` SET label='renamed' WHERE id=1")
	wait(t, ctx, func() bool {
		var v string
		err := a.Sink.Conn.QueryRow(ctx, "SELECT label FROM `"+renamed+"` FINAL WHERE _migres_deleted=0").Scan(&v)
		return err == nil && v == "renamed"
	})
	exec("TRUNCATE TABLE `" + renamed + "`")
	wait(t, ctx, func() bool {
		var n uint64
		err := a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM `"+renamed+"` FINAL").Scan(&n)
		return err == nil && n == 0
	})
}

package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/app"
	"github.com/arm092/migres-go/internal/source"
)

func TestAlterDefaultBinaryLiterals(t *testing.T) {
	c := testConfig(t)
	name := fmt.Sprintf("binary_defaults_%d", time.Now().UnixNano())
	c.MySQL.Include = []string{name}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	m, err := source.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	exec := func(sql string) {
		t.Helper()
		if _, e := m.DB.ExecContext(ctx, sql); e != nil {
			t.Fatal(e)
		}
	}
	exec("CREATE TABLE `" + name + "`(id INT PRIMARY KEY,flag BIT(1) NOT NULL DEFAULT b'1',raw VARBINARY(2) NOT NULL DEFAULT x'4142')")
	defer m.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
	exec("INSERT INTO `" + name + "`(id) VALUES(1)")
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
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	wait(t, ctx, func() bool {
		var count uint64
		return a.Ready.Load() && a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM `"+name+"` FINAL WHERE id=1 AND flag=1 AND hex(raw)='4142'").Scan(&count) == nil && count == 1
	})
	exec("ALTER TABLE `" + name + "` ADD COLUMN extra BIT(1) NOT NULL DEFAULT b'1', ADD COLUMN bytes VARBINARY(2) NOT NULL DEFAULT x'00FF'")
	exec("ALTER TABLE `" + name + "` ALTER COLUMN flag SET DEFAULT b'0', ALTER COLUMN raw SET DEFAULT x'4344'")
	exec("INSERT INTO `" + name + "`(id) VALUES(2)")
	wait(t, ctx, func() bool {
		var count uint64
		err := a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM `"+name+"` FINAL WHERE _migres_deleted=0 AND extra=1 AND hex(bytes)='00FF' AND ((id=1 AND flag=1 AND hex(raw)='4142') OR (id=2 AND flag=0 AND hex(raw)='4344'))").Scan(&count)
		return err == nil && count == 2
	})
}

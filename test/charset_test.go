package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/app"
	"github.com/arm092/migres-go/internal/source"
)

func TestTextCharsetSnapshotCDCAndDDL(t *testing.T) {
	c := testConfig(t)
	name := fmt.Sprintf("charset_%d", time.Now().UnixNano())
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
		if _, err := m.DB.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE TABLE `" + name + "` (id VARCHAR(40) PRIMARY KEY, value TEXT, raw VARBINARY(10)) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin")
	defer m.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
	exec("INSERT INTO `" + name + "` VALUES ('é','café €',X'00FF')")
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
	verify := func(key, value string) {
		t.Helper()
		wait(t, ctx, func() bool {
			var count uint64
			var id, v, raw string
			if a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&count) != nil || count != 1 {
				return false
			}
			err := a.Sink.Conn.QueryRow(ctx, "SELECT id,value,hex(raw) FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&id, &v, &raw)
			return err == nil && id == key && v == value && raw == "00FF"
		})
	}
	verify("é", "café €")
	exec("UPDATE `" + name + "` SET value='après €'")
	verify("é", "après €")
	exec("UPDATE `" + name + "` SET id='ñ',value='mañana'")
	verify("ñ", "mañana")
	exec("ALTER TABLE `" + name + "` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
	exec("UPDATE `" + name + "` SET id='Հայ',value='Հայերեն'")
	verify("Հայ", "Հայերեն")
}

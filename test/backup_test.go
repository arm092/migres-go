package test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/app"
	"github.com/arm092/migres-go/internal/journal"
	"github.com/arm092/migres-go/internal/source"
)

// A stopped journal is restored with the unchanged target. The source advances
// while Migres is offline; restore must resume CDC, including key changes/deletes.
func TestBackupRestoreCDC(t *testing.T) {
	c := testConfig(t)
	name := fmt.Sprintf("backup_%d", time.Now().UnixNano())
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
	exec("CREATE TABLE `" + name + "` (tenant INT, id INT, value VARCHAR(30), PRIMARY KEY(tenant,id))")
	defer m.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
	exec("INSERT INTO `" + name + "` VALUES(1,1,'before'),(2,1,'delete')")
	a, err := app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- a.Run(runCtx) }()
	wait(t, ctx, func() bool {
		var n uint64
		return a.Ready.Load() && a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&n) == nil && n == 2
	})
	stop()
	if err = <-done; err != nil {
		a.Close()
		t.Fatal(err)
	}
	before, err := a.Store.Stats()
	if err != nil {
		a.Close()
		t.Fatal(err)
	}
	a.Close()
	backup := filepath.Join(t.TempDir(), "backup.db")
	if err = journal.ExportOffline(filepath.Join(c.DataDir, "journal.db"), backup, false); err != nil {
		t.Fatal(err)
	}
	exec("UPDATE `" + name + "` SET id=2,value='after' WHERE tenant=1")
	exec("DELETE FROM `" + name + "` WHERE tenant=2")
	exec("INSERT INTO `" + name + "` VALUES(3,3,'offline')")
	c.DataDir = t.TempDir()
	c.RuntimeDir = filepath.Join(c.DataDir, "run")
	if err = journal.ExportOffline(backup, filepath.Join(c.DataDir, "journal.db"), true); err != nil {
		t.Fatal(err)
	}
	b, err := app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	restored, err := b.Store.Stats()
	if err != nil || restored.Checkpoint != before.Checkpoint {
		t.Fatal("restore checkpoint mismatch", restored, before, err)
	}
	runCtx, stop = context.WithCancel(ctx)
	go func() { done <- b.Run(runCtx) }()
	defer func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	wait(t, ctx, func() bool {
		var n, matching uint64
		err := b.Sink.Conn.QueryRow(ctx, "SELECT count(),countIf((tenant=1 AND id=2 AND value='after') OR (tenant=3 AND id=3 AND value='offline')) FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&n, &matching)
		return err == nil && n == 2 && matching == 2
	})
}

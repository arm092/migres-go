package test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/app"
	"github.com/arm092/migres-go/internal/config"
	"github.com/arm092/migres-go/internal/source"
)

func TestCrashHelper(t *testing.T) {
	raw := os.Getenv("MIGRES_CRASH_CONFIG")
	if raw == "" {
		t.Skip("subprocess helper")
	}
	var c config.Config
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		t.Fatal(err)
	}
	a, err := app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err = a.Serve(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// Killing the actual journal owner exercises recovery without deferred cleanup.
func TestProcessKillRecovery(t *testing.T) {
	c := testConfig(t)
	c.Migration.BatchRows = 20
	name := fmt.Sprintf("crash_%d", time.Now().UnixNano())
	c.MySQL.Include = []string{name}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	m, err := source.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if _, err = m.DB.ExecContext(ctx, "CREATE TABLE `"+name+"` (tenant INT NOT NULL,id INT NOT NULL,value BIGINT NOT NULL,PRIMARY KEY(tenant,id))"); err != nil {
		t.Fatal(err)
	}
	defer m.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
	tx, err := m.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := 0; i < 500; i++ {
		if _, err = tx.ExecContext(ctx, "INSERT INTO `"+name+"` VALUES(?,?,?)", i%3, i, i); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(c)
	log, err := os.Create(filepath.Join(t.TempDir(), "child.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCrashHelper$", "-test.timeout=2m")
	child.Env = append(os.Environ(), "MIGRES_CRASH_CONFIG="+string(raw))
	child.Stdout = log
	child.Stderr = log
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	defer child.Process.Kill()
	time.Sleep(1200 * time.Millisecond)
	if _, err = m.DB.ExecContext(ctx, "UPDATE `"+name+"` SET value=value+1000 WHERE id<250"); err != nil {
		t.Fatal(err)
	}
	if _, err = m.DB.ExecContext(ctx, "DELETE FROM `"+name+"` WHERE id>=400"); err != nil {
		t.Fatal(err)
	}
	if err = child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	child.Wait() // an unclean exit is expected
	if _, err = m.DB.ExecContext(ctx, "FLUSH BINARY LOGS"); err != nil {
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
		if t.Failed() {
			st, _ := a.Store.Stats()
			var diagnostic string
			a.Store.Meta("last_error", &diagnostic)
			t.Logf("ready=%v blocked=%v stats=%+v diagnostic=%s", a.Ready.Load(), a.Blocked.Load(), st, diagnostic)
			data, _ := os.ReadFile(log.Name())
			t.Logf("child: %s", data)
		}
	}()
	defer func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	wait(t, ctx, func() bool {
		var count uint64
		var sum int64
		err := a.Sink.Conn.QueryRow(ctx, "SELECT count(),sum(value) FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&count, &sum)
		return a.Ready.Load() && err == nil && count == 400 && sum == 329800
	})
}

package test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err = a.Serve(ctx); err != nil {
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

func TestSnapshotInterrupted(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Linux signal semantics")
	}
	for _, mode := range []string{"gtid", "signal"} {
		for _, kill := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s_kill_%v", mode, kill), func(t *testing.T) {
				c := testConfig(t)
				c.Migration.Mode = "snapshot"
				c.Migration.SnapshotMode = mode
				c.Migration.BatchRows = 1
				c.Migration.CDC.BatchMaxWait = 20 * time.Millisecond
				name := fmt.Sprintf("snapstop_%d", time.Now().UnixNano())
				c.MySQL.Include = []string{name}
				ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer cancel()
				m, err := source.Open(c)
				if err != nil {
					t.Fatal(err)
				}
				defer m.Close()
				var enabled string
				if err = m.DB.QueryRowContext(ctx, "SELECT @@gtid_mode").Scan(&enabled); err != nil {
					t.Fatal(err)
				}
				if mode == "gtid" && enabled != "ON" {
					t.Skip("GTID disabled")
				}
				if _, err = m.DB.ExecContext(ctx, "CREATE TABLE `"+name+"`(id INT PRIMARY KEY,value INT NOT NULL)"); err != nil {
					t.Fatal(err)
				}
				defer m.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
				tx, err := m.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 300; i++ {
					if _, err = tx.ExecContext(ctx, "INSERT INTO `"+name+"` VALUES(?,?)", i, i*17); err != nil {
						tx.Rollback()
						t.Fatal(err)
					}
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				c.Listen = listener.Addr().String()
				listener.Close()
				raw, _ := json.Marshal(c)
				child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCrashHelper$", "-test.timeout=1m")
				child.Env = append(os.Environ(), "MIGRES_CRASH_CONFIG="+string(raw))
				var output strings.Builder
				child.Stdout = &output
				child.Stderr = &output
				if err = child.Start(); err != nil {
					t.Fatal(err)
				}
				defer child.Process.Kill()
				wait(t, ctx, func() bool {
					r, e := http.Get("http://" + c.Listen + "/metrics")
					if e != nil {
						return false
					}
					defer r.Body.Close()
					b, _ := io.ReadAll(r.Body)
					return strings.Contains(string(b), "migres_snapshot_running 1")
				})
				// A concurrent transaction is either copied or caught up, including key deletes.
				if _, err = m.DB.ExecContext(ctx, "UPDATE `"+name+"` SET value=value+100 WHERE id<100"); err != nil {
					t.Fatal(err)
				}
				if kill {
					err = child.Process.Kill()
				} else {
					err = child.Process.Signal(syscall.SIGTERM)
				}
				if err != nil {
					t.Fatal(err)
				}
				err = child.Wait()
				if !kill && err != nil {
					t.Fatalf("SIGTERM not graceful: %v %s", err, output.String())
				}
				c.Migration.BatchRows = 100
				a, err := app.Open(c)
				if err != nil {
					t.Fatal(err)
				}
				defer a.Close()
				if err = a.Run(ctx); err != nil {
					t.Fatal(err)
				}
				var count, bad uint64
				if err = a.Sink.Conn.QueryRow(ctx, "SELECT count(),countIf(value != id*17+if(id<100,100,0)) FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&count, &bad); err != nil || count != 300 || bad != 0 {
					t.Fatalf("snapshot mismatch %d bad=%d %v", count, bad, err)
				}
			})
		}
	}
}

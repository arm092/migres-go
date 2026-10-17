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
	"github.com/arm092/migres-go/internal/sink"
	"github.com/arm092/migres-go/internal/source"
)

// Opt-in acceptance run. Every cycle reconciles all keys and values; periodic
// process kills, rollbacks and rotations exercise the persistent boundaries.
func TestSoak(t *testing.T) {
	rawDuration := os.Getenv("MIGRES_SOAK_DURATION")
	if rawDuration == "" {
		t.Skip("set MIGRES_SOAK_DURATION=24h for acceptance")
	}
	duration, err := time.ParseDuration(rawDuration)
	if err != nil || duration <= 0 {
		t.Fatal("invalid soak duration")
	}
	c := testConfig(t)
	c.Migration.CDC.ServerID = 64380
	c.Migration.BatchRows = 25
	c.Migration.CDC.ProducerFlush = 50 * time.Millisecond
	c.Migration.CDC.BatchMaxWait = 50 * time.Millisecond
	name := fmt.Sprintf("soak_%d", time.Now().UnixNano())
	c.MySQL.Include = []string{name}
	ctx, cancel := context.WithTimeout(context.Background(), duration+2*time.Minute)
	defer cancel()
	m, err := source.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if _, err = m.DB.ExecContext(ctx, "CREATE TABLE `"+name+"`(tenant BIGINT NOT NULL,id BIGINT NOT NULL,value BIGINT NOT NULL,PRIMARY KEY(tenant,id))"); err != nil {
		t.Fatal(err)
	}
	defer m.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
	peer, err := sink.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	log, err := os.Create(filepath.Join(t.TempDir(), "owner.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	configJSON, _ := json.Marshal(c)
	var child *exec.Cmd
	kill := func() {
		if child != nil {
			child.Process.Kill()
			child.Wait()
			child = nil
		}
	}
	defer func() {
		kill()
		a, e := app.Open(c)
		if e != nil {
			t.Error(e)
			return
		}
		defer a.Close()
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
	start := func() {
		child = exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCrashHelper$", "-test.timeout=25h")
		child.Env = append(os.Environ(), "MIGRES_CRASH_CONFIG="+string(configJSON))
		child.Stdout = log
		child.Stderr = log
		if err := child.Start(); err != nil {
			t.Fatal(err)
		}
	}
	start()
	started := time.Now()
	for cycle := 1; time.Since(started) < duration; cycle++ {
		tx, e := m.DB.BeginTx(ctx, nil)
		if e != nil {
			t.Fatal(e)
		}
		for i := 0; i < 80; i++ {
			id := (cycle*17 + i) % 200
			if _, e = tx.ExecContext(ctx, "INSERT INTO `"+name+"` VALUES(?,?,?) ON DUPLICATE KEY UPDATE value=VALUES(value)", id%3, id, cycle*100+i); e != nil {
				tx.Rollback()
				t.Fatal(e)
			}
		}
		if _, e = tx.ExecContext(ctx, "DELETE FROM `"+name+"` WHERE id%13=?", cycle%13); e != nil {
			tx.Rollback()
			t.Fatal(e)
		}
		if cycle%5 == 0 {
			e = tx.Rollback()
		} else {
			e = tx.Commit()
		}
		if e != nil {
			t.Fatal(e)
		}
		if cycle%7 == 0 {
			kill()
			start()
		}
		if cycle%30 == 0 {
			if _, e = m.DB.ExecContext(ctx, "FLUSH BINARY LOGS"); e != nil {
				t.Fatal(e)
			}
		}
		rows, e := m.DB.QueryContext(ctx, "SELECT tenant,id,value FROM `"+name+"` ORDER BY tenant,id")
		if e != nil {
			t.Fatal(e)
		}
		var expected [][3]int64
		for rows.Next() {
			var r [3]int64
			if e = rows.Scan(&r[0], &r[1], &r[2]); e != nil {
				rows.Close()
				t.Fatal(e)
			}
			expected = append(expected, r)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			t.Fatal(e)
		}
		cycleCtx, stop := context.WithTimeout(ctx, 45*time.Second)
		wait(t, cycleCtx, func() bool {
			r, e := peer.Conn.Query(cycleCtx, "SELECT tenant,id,value FROM `"+name+"` FINAL WHERE _migres_deleted=0 ORDER BY tenant,id")
			if e != nil {
				return false
			}
			defer r.Close()
			var actual [][3]int64
			for r.Next() {
				var v [3]int64
				if e = r.Scan(&v[0], &v[1], &v[2]); e != nil {
					return false
				}
				actual = append(actual, v)
			}
			return r.Err() == nil && fmt.Sprint(actual) == fmt.Sprint(expected)
		})
		stop()
		if cycle%30 == 0 {
			t.Logf("elapsed=%s cycles=%d exact reconciliation passed", time.Since(started).Round(time.Second), cycle)
		}
	}
}

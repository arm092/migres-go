package app

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/arm092/migres-go/internal/config"
)

// Keep all real source, journal and sink behavior, but hold an INSERT at the
// driver boundary until the application's force context expires.
type shutdownInsertGate struct {
	driver.Conn
	started chan struct{}
	once    sync.Once
}

func (g *shutdownInsertGate) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	g.once.Do(func() { close(g.started) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestShutdownDeadlinePreservesPendingWork(t *testing.T) {
	if os.Getenv("MIGRES_INTEGRATION") != "1" {
		t.Skip("isolated fixture required")
	}
	for _, stage := range []string{"snapshot", "insert", "transform", "rebuild"} {
		t.Run(stage, func(t *testing.T) {
			c := config.Default()
			if host := os.Getenv("MIGRES_TEST_HOST"); host != "" {
				c.MySQL.Host = host
				c.ClickHouse.Host = host
			}
			c.MySQL.Port = 33080
			c.MySQL.User = "root"
			c.MySQL.Password = "test-only"
			c.MySQL.Database = "migres_src"
			c.ClickHouse.Port = 39000
			c.ClickHouse.Password = "test-only"
			c.ClickHouse.Database = "migres_dst"
			c.DataDir = t.TempDir()
			c.ShutdownTimeout = 200 * time.Millisecond
			c.Migration.CDC.ServerID = 100000 + rand.Uint32()%3000000000
			c.Migration.SnapshotMode = "gtid"
			c.Migration.CDC.BatchMaxWait = 10 * time.Millisecond
			c.Migration.CDC.TransformerPoll = time.Millisecond
			c.Migration.CDC.ConsumerPoll = time.Millisecond
			if stage == "transform" {
				c.Migration.CDC.BatchMaxWait = time.Hour
			}
			name := fmt.Sprintf("shutdown_%s_%d", stage, time.Now().UnixNano())
			c.MySQL.Include = []string{name}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			a, err := Open(c)
			if err != nil {
				t.Fatal(err)
			}
			current := a
			defer func() { current.Close() }()
			sql := func(q string) {
				t.Helper()
				if _, e := a.Source.DB.ExecContext(ctx, q); e != nil {
					t.Fatal(e)
				}
			}
			sql("CREATE TABLE `" + name + "` (id BIGINT PRIMARY KEY,value INT NOT NULL)")
			defer func() { current.Source.DB.Exec("DROP TABLE IF EXISTS `" + name + "`") }()
			sql("INSERT INTO `" + name + "` VALUES(1,42)")
			if err = a.Check(ctx); err != nil {
				t.Fatal(err)
			}
			if err = a.bootstrap(ctx); err != nil {
				t.Fatal(err)
			}
			defer func() {
				owned, e := current.Store.Owned()
				if e != nil {
					t.Error(e)
					return
				}
				for _, s := range owned {
					if e = current.Sink.DropOwned(context.Background(), s); e != nil {
						t.Error(e)
					}
				}
			}()
			if stage != "snapshot" {
				if err = a.Store.SetMeta("snapshot_done", true); err != nil {
					t.Fatal(err)
				}
			}
			if stage == "rebuild" {
				// Advance source metadata before Run, retaining the old durable schema so
				// the producer emits the same rebuild event as live CDC would.
				sql("ALTER TABLE `" + name + "` MODIFY value BIGINT NOT NULL")
			}
			gate := &shutdownInsertGate{Conn: a.Sink.Conn, started: make(chan struct{})}
			if stage != "transform" {
				a.Sink.Conn = gate
			}
			runCtx, stop := context.WithCancel(ctx)
			defer stop()
			done := make(chan error, 1)
			go func() { done <- a.Run(runCtx) }()
			await := func(fn func() bool) {
				t.Helper()
				for !fn() {
					select {
					case e := <-done:
						t.Fatalf("pipeline exited before cancellation: %v", e)
					case <-ctx.Done():
						t.Fatal("stage deadline")
					case <-time.After(5 * time.Millisecond):
					}
				}
			}
			if stage == "insert" || stage == "transform" {
				await(func() bool { return a.Ready.Load() })
				sql("UPDATE `" + name + "` SET value=84 WHERE id=1")
			}
			if stage == "transform" {
				await(func() bool {
					events, e := a.Store.Events(100)
					if e != nil {
						t.Fatal(e)
					}
					for _, event := range events {
						if event.Table == name && event.Kind == "update" {
							return true
						}
					}
					return false
				})
			} else {
				await(func() bool {
					select {
					case <-gate.started:
						return true
					default:
						return false
					}
				})
			}
			if stage == "snapshot" && !a.SnapshotRunning.Load() {
				t.Fatal("snapshot already completed at blocked INSERT")
			}
			if stage == "rebuild" && !a.RebuildRunning.Load() {
				t.Fatal("rebuild not active at blocked INSERT")
			}
			start := time.Now()
			stop()
			select {
			case err = <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("shutdown exceeded bounded return")
			}
			elapsed := time.Since(start)
			if stage != "transform" && elapsed < c.ShutdownTimeout/2 {
				t.Fatalf("INSERT escaped force deadline: %s", elapsed)
			}
			r := httptest.NewRecorder()
			a.Handler().ServeHTTP(r, httptest.NewRequest("GET", "/readyz", nil))
			if r.Code != 503 {
				t.Fatalf("shutdown readiness=%d", r.Code)
			}
			if a.ProducerRunning.Load() || a.TransformerRunning.Load() || a.ConsumerRunning.Load() || a.SnapshotRunning.Load() || a.RebuildRunning.Load() {
				t.Fatal("stage remains running after return")
			}
			stats, err := a.Store.Stats()
			if err != nil {
				t.Fatal(err)
			}
			if stats.Events+stats.Tasks == 0 {
				t.Fatal("cancelled pending work was lost")
			}
			var rebuildToken string
			if stage == "rebuild" {
				tasks, e := a.Store.Tasks(100)
				if e != nil {
					t.Fatal(e)
				}
				for _, task := range tasks {
					if task.Rebuild {
						rebuildToken = task.Token
						var state rebuildState
						if e = a.Store.Meta("rebuild:"+task.Token, &state); e != nil || state.Step != "copy" {
							t.Fatalf("rebuild checkpoint=%+v %v", state, e)
						}
					}
				}
				if rebuildToken == "" {
					t.Fatal("missing durable rebuild task")
				}
			}
			// Close and reopen the journal: recovery must use persisted state, not the
			// first application's memory or an artificially released successful INSERT.
			a.Close()
			c.Migration.CDC.BatchMaxWait = 10 * time.Millisecond
			a, err = Open(c)
			if err != nil {
				t.Fatal(err)
			}
			current = a
			restartCtx, restartStop := context.WithCancel(ctx)
			defer restartStop()
			restarted := make(chan error, 1)
			go func() { restarted <- a.Run(restartCtx) }()
			want := int64(42)
			if stage == "insert" || stage == "transform" {
				want = 84
			}
			for {
				var value int64
				e := a.Sink.Conn.QueryRow(ctx, "SELECT toInt64(value) FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&value)
				if e == nil && value == want && a.Ready.Load() {
					break
				}
				select {
				case e := <-restarted:
					t.Fatalf("restart exited: %v", e)
				case <-ctx.Done():
					t.Fatal("restart reconciliation deadline")
				case <-time.After(10 * time.Millisecond):
				}
			}
			restartStop()
			select {
			case e := <-restarted:
				if e != nil {
					t.Fatal(e)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("restart shutdown deadline")
			}
			if rebuildToken != "" {
				var state rebuildState
				if e := a.Store.Meta("rebuild:"+rebuildToken, &state); e != nil || state.Step != "done" {
					t.Fatalf("rebuild did not finish: %+v %v", state, e)
				}
			}
			t.Logf("stage=%s shutdown=%s durable_events=%d durable_tasks=%d restart_value=%d", stage, elapsed, stats.Events, stats.Tasks, want)
		})
	}
}

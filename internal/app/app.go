// Package app coordinates the lifetimes of Producer, Transformer and Consumer.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/arm092/migres-go/internal/config"
	"github.com/arm092/migres-go/internal/journal"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/notify"
	"github.com/arm092/migres-go/internal/pipeline"
	"github.com/arm092/migres-go/internal/sink"
	"github.com/arm092/migres-go/internal/source"
)

type App struct {
	Config             config.Config
	Store              *journal.Store
	Source             *source.MySQL
	Sink               *sink.ClickHouse
	Ready              atomic.Bool
	Blocked            atomic.Bool
	ProducerRunning    atomic.Bool
	TransformerRunning atomic.Bool
	ConsumerRunning    atomic.Bool
	SnapshotRunning    atomic.Bool
	DDLRunning         atomic.Bool
	RebuildRunning     atomic.Bool
	Notifier           *notify.Notifier
}

func Open(c config.Config) (*App, error) {
	s, err := journal.Open(filepath.Join(c.DataDir, "journal.db"), time.Second)
	if err != nil {
		return nil, err
	}
	m, err := source.Open(c)
	if err != nil {
		s.Close()
		return nil, err
	}
	ch, err := sink.Open(c)
	if err != nil {
		m.Close()
		s.Close()
		return nil, err
	}
	s.MaxTransactionBytes = c.MaxTransactionBytes
	ch.Store = s
	return &App{Config: c, Store: s, Source: m, Sink: ch, Notifier: notify.New(c)}, nil
}
func (a *App) Close() { a.Sink.Close(); a.Source.Close(); a.Store.Close() }
func (a *App) Check(ctx context.Context) error {
	if err := a.Source.Check(ctx); err != nil {
		return err
	}
	if err := a.Sink.Check(ctx); err != nil {
		return err
	}
	var uuid string
	if err := a.Source.DB.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil {
		return err
	}
	var old string
	if err := a.Store.Meta("source_uuid", &old); err == nil && uuid != old {
		return fmt.Errorf("source identity changed; explicit recovery required")
	}
	var charset string
	if err := a.Store.Meta("database_charset", &charset); err == nil && charset != a.Source.DatabaseCharset {
		return fmt.Errorf("source database default charset changed; explicit reset and resnapshot required")
	}
	if err := a.Store.SetMeta("database_charset", a.Source.DatabaseCharset); err != nil {
		return err
	}
	return a.Store.SetMeta("source_uuid", uuid)
}
func (a *App) bootstrap(ctx context.Context) error {
	var p model.Position
	err := a.Store.Meta("checkpoint", &p)
	if err == nil {
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	// Metadata capture must not overlap any source transaction. Retry a bounded
	// number of times rather than attach an unknown current schema to old rows.
	for attempt := 0; attempt < 5; attempt++ {
		tables, err := a.Source.Tables(ctx)
		if err != nil {
			return err
		}
		var schemas []model.Schema
		for _, table := range tables {
			s, err := a.Source.Schema(ctx, table)
			if err != nil {
				return err
			}
			if existing, e := a.Store.Schema(table); e == nil {
				s.Generation = existing.Generation
			}
			schemas = append(schemas, s)
		}
		before, err := a.Source.Position(ctx)
		if err != nil {
			return err
		}
		stable := true
		again, err := a.Source.Tables(ctx)
		if err != nil {
			return err
		}
		if len(again) != len(tables) {
			continue
		}
		for i, s := range schemas {
			if again[i] != s.Table {
				stable = false
				break
			}
			fresh, e := a.Source.Schema(ctx, s.Table)
			if e != nil {
				return e
			}
			if fresh.SourceDDL != s.SourceDDL {
				stable = false
				break
			}
		}
		if !stable {
			continue
		}
		for _, s := range schemas {
			if err = a.Store.SaveSchema(s); err != nil {
				return err
			}
			if err = a.Sink.Ensure(ctx, s); err != nil {
				return err
			}
		}
		if err = a.Store.SetMeta("initial_schemas", schemas); err != nil {
			return err
		}
		if err = a.Store.SetMeta("initial_position", before); err != nil {
			return err
		}
		return a.Store.Append(nil, before)
	}
	return fmt.Errorf("source schema bootstrap raced writes; retry initialization during a quiet metadata interval")
}
func (a *App) Run(ctx context.Context) error {
	if err := a.recoverOperations(ctx); err != nil {
		return err
	}
	if err := a.Check(ctx); err != nil {
		return err
	}
	snap := &pipeline.Snapshot{Config: a.Config, Store: a.Store, Source: a.Source}
	needSnapshot := a.Config.Migration.Mode == "snapshot" || a.Config.Migration.CDC.SnapshotBefore
	var done bool
	a.Store.Meta("snapshot_done", &done)
	if done && a.Config.Migration.Mode == "snapshot" {
		return nil
	}
	if needSnapshot && !done {
		if err := snap.Prepare(ctx); err != nil {
			return err
		}
	}
	if err := a.bootstrap(ctx); err != nil {
		return err
	}
	a.Blocked.Store(false)
	if err := a.Store.SetMeta("last_error", ""); err != nil {
		return err
	}
	pctx, pstop := context.WithCancel(context.Background())
	tctx, tstop := context.WithCancel(context.Background())
	cctx, cstop := context.WithCancel(context.Background())
	defer pstop()
	defer tstop()
	defer cstop()
	forceCtx, forceStop := context.WithCancel(context.Background())
	defer forceStop()
	pDone := make(chan error, 1)
	tDone := make(chan error, 1)
	cDone := make(chan error, 1)
	snapDone := make(chan error, 1)
	a.ProducerRunning.Store(true)
	a.TransformerRunning.Store(true)
	a.ConsumerRunning.Store(true)
	go func() {
		defer a.ProducerRunning.Store(false)
		pDone <- (&pipeline.Producer{Config: a.Config, Store: a.Store, Source: a.Source}).Run(pctx)
	}()
	go func() {
		defer a.TransformerRunning.Store(false)
		tDone <- (&pipeline.Transformer{Store: a.Store, Poll: a.Config.Migration.CDC.TransformerPoll, Debug: a.Config.Migration.CDC.DBDebug, BatchRows: a.Config.Migration.BatchRows, CheckpointRows: a.Config.Migration.CDC.CheckpointRows, MaxWait: a.Config.Migration.CDC.BatchMaxWait}).Run(tctx)
	}()
	go func() {
		defer a.ConsumerRunning.Store(false)
		cDone <- (&pipeline.Consumer{Store: a.Store, Target: a, Workers: a.Config.Migration.ConsumerWorkers, Poll: a.Config.Migration.CDC.ConsumerPoll, Force: forceCtx, BatchLimit: a.Config.Migration.CDC.PreparedLimit}).Run(cctx)
	}()
	if needSnapshot && !done {
		a.SnapshotRunning.Store(true)
		go func() { defer a.SnapshotRunning.Store(false); snapDone <- snap.Run(pctx) }()
	} else {
		var pending bool
		a.Store.Meta("resnapshot_pending", &pending)
		a.Ready.Store(!pending)
	}
	a.Notifier.Send(ctx, "started", "Pipeline started")
	var failure error
	var pExited, tExited, cExited bool
	snapshotActive := needSnapshot && !done
running:
	for {
		select {
		case <-ctx.Done():
			break running
		case err := <-pDone:
			pExited = true
			failure = fmt.Errorf("Producer stopped: %w", err)
			break running
		case err := <-tDone:
			tExited = true
			failure = fmt.Errorf("Transformer stopped: %w", err)
			break running
		case err := <-cDone:
			cExited = true
			failure = fmt.Errorf("Consumer stopped: %w", err)
			break running
		case err := <-snapDone:
			snapshotActive = false
			if err != nil {
				failure = fmt.Errorf("snapshot: %w", err)
				break running
			}
			if a.Config.Migration.Mode == "snapshot" {
				break running
			}
			a.Ready.Store(true)

		}
	}
	a.Ready.Store(false)
	if failure != nil {
		a.Blocked.Store(true)
		a.Store.SetMeta("last_error", failure.Error())
		slog.Error("pipeline blocked", "error", pipeline.ErrorSummary(failure))
		a.Notifier.Send(context.Background(), "blocked", "Pipeline stopped; inspect local logs and durable queues")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.Config.ShutdownTimeout)
	defer cancel()
	go func() { <-shutdownCtx.Done(); forceStop(); pstop(); tstop(); cstop() }()
	wait := func(ch <-chan error, exited bool) {
		if exited {
			return
		}
		<-ch
	}
	pstop()
	wait(pDone, pExited)
	if snapshotActive {
		<-snapDone
	}
	tstop()
	wait(tDone, tExited)
	cstop()
	wait(cDone, cExited)
	a.Notifier.Send(context.Background(), "stopped", "Pipeline stopped; unacknowledged work remains durable")
	return failure
}
func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !a.Ready.Load() {
			http.Error(w, "not ready", 503)
			return
		}
		w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		s, err := a.Store.StorageStats()
		if err != nil {
			http.Error(w, "journal unavailable", 503)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "migres_ready %d\nmigres_blocked %d\nmigres_raw_events %d\nmigres_prepared_queries %d\nmigres_journal_bytes %d\n", boolInt(a.Ready.Load()), boolInt(a.Blocked.Load()), s.Events, s.Tasks, s.Bytes)
		var retries, delivered uint64
		a.Store.Meta("consumer_retries", &retries)
		a.Store.Meta("delivered_rows", &delivered)
		fmt.Fprintf(w, "migres_storage_bytes %d\nmigres_staged_events %d\nmigres_oldest_pending_age_seconds %d\nmigres_consumer_retries_total %d\nmigres_delivered_rows_total %d\n", s.JournalBytes, s.StagedEvents, s.OldestAgeSeconds, retries, delivered)
		fmt.Fprintf(w, "migres_journal_reusable_bytes %d\nmigres_journal_history_bytes %d\nmigres_journal_history_entries %d\n", s.ReusableBytes, s.HistoryBytes, s.HistoryEntries)
		var snapshotDone bool
		a.Store.Meta("snapshot_done", &snapshotDone)
		fmt.Fprintf(w, "migres_stage_running{stage=\"producer\"} %d\nmigres_stage_running{stage=\"transformer\"} %d\nmigres_stage_running{stage=\"consumer\"} %d\nmigres_snapshot_running %d\nmigres_snapshot_complete %d\nmigres_ddl_running %d\nmigres_rebuild_running %d\n", boolInt(a.ProducerRunning.Load()), boolInt(a.TransformerRunning.Load()), boolInt(a.ConsumerRunning.Load()), boolInt(a.SnapshotRunning.Load()), boolInt(snapshotDone), boolInt(a.DDLRunning.Load()), boolInt(a.RebuildRunning.Load()))
		if a.Config.ClickHouse.Engine == "ReplicatedReplacingMergeTree" {
			probeCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			var tables, readonly, queued, delay uint64
			err := a.Sink.Conn.QueryRow(probeCtx, "SELECT count(),toUInt64(sum(is_readonly)),toUInt64(sum(queue_size)),toUInt64(max(absolute_delay)) FROM system.replicas WHERE database=?", a.Config.ClickHouse.Database).Scan(&tables, &readonly, &queued, &delay)
			fmt.Fprintf(w, "migres_replication_probe_up %d\nmigres_expected_replicas %d\n", boolInt(err == nil), len(a.Config.ClickHouse.Addresses))
			if err == nil {
				fmt.Fprintf(w, "migres_replica_tables %d\nmigres_replica_readonly_tables %d\nmigres_replica_queue_size %d\nmigres_replica_delay_seconds %d\n", tables, readonly, queued, delay)
			}
		}
	})
	return mux
}
func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

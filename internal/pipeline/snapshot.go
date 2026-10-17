package pipeline

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/arm092/migres-go/internal/config"
	"github.com/arm092/migres-go/internal/journal"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/schema"
	"github.com/arm092/migres-go/internal/source"
	gm "github.com/go-mysql-org/go-mysql/mysql"
)

type Snapshot struct {
	Config  config.Config
	Store   *journal.Store
	Source  *source.MySQL
	Signals bool
}

func (s *Snapshot) Prepare(ctx context.Context) error {
	mode := s.Config.Migration.SnapshotMode
	var gtid string
	if err := s.Source.DB.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_mode").Scan(&gtid); err != nil {
		return err
	}
	if mode == "gtid" && gtid != "ON" {
		return fmt.Errorf("GTID snapshot requires gtid_mode=ON")
	}
	if mode == "signal" || (mode == "auto" && gtid != "ON") {
		s.Signals = true
		// Verify write access before taking a baseline. This small dedicated transaction
		// is independent of business data and never locks a business table.
		q := "INSERT INTO " + schema.Ref(s.Config.MySQL.Database, s.Config.Migration.SignalTable) + " (id, marker) VALUES (?, ?) ON DUPLICATE KEY UPDATE marker=VALUES(marker)"
		if _, err := s.Source.DB.ExecContext(ctx, q, "probe", journal.ID()); err != nil {
			return fmt.Errorf("snapshot requires GTID or writable prepared signal table: %w", err)
		}
	}
	return nil
}
func (s *Snapshot) Run(ctx context.Context) error {
	tables, err := s.Source.Tables(ctx)
	if err != nil {
		return err
	}
	jobs := make(chan string)
	result := make(chan error, s.Config.Migration.Workers)
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < s.Config.Migration.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for table := range jobs {
				if err := s.table(workCtx, table); err != nil {
					select {
					case result <- err:
					default:
					}
					cancel()
					return
				}
			}
		}()
	}
feed:
	for _, table := range tables {
		select {
		case jobs <- table:
		case <-workCtx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()
	close(result)
	for err := range result {
		if err != nil {
			return err
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	boundary, err := s.Source.Position(ctx)
	if err != nil {
		return err
	}
	if err = s.Store.SetMeta("snapshot_boundary", boundary); err != nil {
		return err
	}
	for {
		var current model.Position
		if err = s.Store.Meta("checkpoint", &current); err != nil {
			return err
		}
		if Reached(current, boundary) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	if err = s.Store.Enqueue([]model.Event{{Kind: "fence"}}); err != nil {
		return err
	}
	for {
		var delivered bool
		s.Store.Meta("snapshot_done", &delivered)
		if delivered {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}
func Reached(current, target model.Position) bool {
	if current.File == target.File {
		return current.Pos >= target.Pos
	}
	a := gm.Position{Name: current.File, Pos: current.Pos}
	b := gm.Position{Name: target.File, Pos: target.Pos}
	return a.Compare(b) >= 0
}
func (s *Snapshot) table(ctx context.Context, table string) error {
	p, err := s.Store.Progress(table)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if p.Done {
		return nil
	}
	p.Table = table
	sc, err := s.Store.Schema(table)
	if err != nil {
		return err
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fresh, err := s.Store.Schema(table)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if fresh.Revision != sc.Revision || fresh.Generation != sc.Generation {
			sc = fresh
			p.LastKey = nil
			p.Done = false
		}
		st, err := s.Store.Stats()
		if err != nil {
			return err
		}
		if st.Events >= s.Config.Migration.CDC.RawMax || st.Bytes >= s.Config.MaxQueueBytes {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
			continue
		}
		if err := s.Marker(ctx, table, "open"); err != nil {
			return err
		}
		rows, err := s.Source.Chunk(ctx, sc, p.LastKey, s.Config.Migration.BatchRows)
		if err != nil {
			latest, e := s.Store.Schema(table)
			if e == nil && latest.Revision != sc.Revision {
				continue
			}
			return err
		}
		if err := s.Marker(ctx, table, "close"); err != nil {
			return err
		}
		boundary, err := s.Source.Position(ctx)
		if err != nil {
			return err
		}
		if err = s.await(ctx, boundary); err != nil {
			return err
		}
		if len(rows) == 0 {
			p.Done = true
			return s.Store.SetProgress(p)
		}
		e := model.Event{Kind: "snapshot", Table: table, Schema: &sc, Position: boundary}
		for _, row := range rows {
			e.Rows = append(e.Rows, model.Change{After: row})
		}
		p.Boundary = boundary
		p.LastKey, err = source.Key(sc, rows[len(rows)-1])
		if err != nil {
			return err
		}
		if err = s.Store.SnapshotRows(e, p); err != nil {
			if errors.Is(err, journal.ErrSchemaChanged) {
				p.LastKey = nil
				continue
			}
			return err
		}
	}
}

func (s *Snapshot) await(ctx context.Context, target model.Position) error {
	for {
		var current model.Position
		if err := s.Store.Meta("checkpoint", &current); err != nil {
			return err
		}
		if Reached(current, target) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *Snapshot) Marker(ctx context.Context, table, phase string) error {
	if !s.Signals {
		return nil
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d:%s", s.Config.Migration.CDC.ServerID, table)))
	id := fmt.Sprintf("%x", sum[:16])
	_, err := s.Source.DB.ExecContext(ctx, "INSERT INTO "+schema.Ref(s.Config.MySQL.Database, s.Config.Migration.SignalTable)+" (id,marker) VALUES (?,?) ON DUPLICATE KEY UPDATE marker=VALUES(marker)", id, phase+":"+journal.ID())
	return err
}

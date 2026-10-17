package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/arm092/migres-go/internal/config"
	"github.com/arm092/migres-go/internal/journal"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/schema"
	"github.com/arm092/migres-go/internal/source"
	gm "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type Producer struct {
	Config config.Config
	Store  *journal.Store
	Source *source.MySQL
}

// A renamed source stays tracked even when its new name was not in include.
// Explicit exclusions always win.
func (p *Producer) tracked(table string) bool {
	if table == p.Config.Migration.SignalTable {
		return false
	}
	for _, name := range p.Config.MySQL.Exclude {
		if name == table {
			return false
		}
	}
	if p.Config.Allowed(table) {
		return true
	}
	_, err := p.Store.Schema(table)
	return err == nil
}

func (p *Producer) Run(ctx context.Context) error {
	if err := p.Store.Rollback(); err != nil {
		return err
	} // reconnect always starts at last committed transaction
	var pos model.Position
	if err := p.Store.Meta("checkpoint", &pos); err != nil {
		return fmt.Errorf("missing initial checkpoint: %w", err)
	}
	c := p.Config
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{ServerID: c.Migration.CDC.ServerID, Flavor: "mysql", Host: c.MySQL.Host, Port: uint16(c.MySQL.Port), User: c.MySQL.User, Password: c.MySQL.Password, TLSConfig: p.Source.TLS, HeartbeatPeriod: c.Migration.CDC.Heartbeat, ReadTimeout: 30 * time.Second, ParseTime: false, TimestampStringLocation: time.UTC, UseDecimal: true, VerifyChecksum: true, EventCacheCount: 8, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	defer syncer.Close()
	var stream *replication.BinlogStreamer
	var err error
	if c.Migration.CDC.UseGTID {
		if pos.GTID == "" {
			return fmt.Errorf("use_gtid requires a saved GTID set")
		}
		set, e := gm.ParseGTIDSet("mysql", pos.GTID)
		if e != nil {
			return e
		}
		stream, err = syncer.StartSyncGTID(set)
	} else {
		stream, err = syncer.StartSync(gm.Position{Name: pos.File, Pos: pos.Pos})
	}
	if err != nil {
		return err
	}
	pending := []model.Event{}
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := p.Store.StageMany(pending); err != nil {
			return err
		}
		pending = nil
		return nil
	}
	defer flush()
	paused := false
	lastDiskCheck := time.Time{}
	file := pos.File
	inTransaction := false
	for {
		if ctx.Err() != nil {
			return nil
		}
		if time.Since(lastDiskCheck) > time.Second {
			if err := p.Store.CheckDisk(uint64(c.MinFreeDiskBytes)); err != nil {
				if inTransaction {
					return fmt.Errorf("insufficient disk during transaction; retained checkpoint requires recovery")
				}
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(time.Second):
				}
				continue
			}
			lastDiskCheck = time.Now()
		}
		stats, err := p.Store.Stats()
		if err != nil {
			return err
		}
		if stats.Events >= c.Migration.CDC.RawMax || stats.Bytes >= c.MaxQueueBytes {
			paused = true
		}
		if paused {
			if stats.Events <= int(float64(c.Migration.CDC.RawMax)*c.Migration.CDC.ResumeRatio) && stats.Bytes <= int64(float64(c.MaxQueueBytes)*c.Migration.CDC.ResumeRatio) {
				paused = false
			} else {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(c.Migration.CDC.ConsumerPoll):
				}
				continue
			}
		}
		eventCtx, eventCancel := context.WithTimeout(ctx, c.Migration.CDC.ProducerFlush)
		ev, err := stream.GetEvent(eventCtx)
		eventCancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, context.DeadlineExceeded) {
				if err = flush(); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("binlog stream: %w", err)
		}
		next := model.Position{File: file, Pos: ev.Header.LogPos, GTID: pos.GTID, Timestamp: int64(ev.Header.Timestamp)}
		switch x := ev.Event.(type) {
		case *replication.RotateEvent:
			file = string(x.NextLogName)
			rotated := model.Position{File: file, Pos: uint32(x.Position), GTID: pos.GTID}
			if !inTransaction && Reached(rotated, pos) && (rotated.File != pos.File || rotated.Pos != pos.Pos) {
				if err = p.Store.Append([]model.Event{{Kind: "position", Position: rotated, SourceID: fmt.Sprintf("safe:%s:%d", file, rotated.Pos)}}, rotated); err != nil {
					return err
				}
				pos = rotated
			}
		case *replication.FormatDescriptionEvent, *replication.PreviousGTIDsEvent, *replication.HeartbeatEvent:
			if !inTransaction && next.Pos > 0 && Reached(next, pos) && (next.File != pos.File || next.Pos != pos.Pos) {
				if err = p.Store.Append([]model.Event{{Kind: "position", Position: next, SourceID: fmt.Sprintf("safe:%s:%d", file, next.Pos)}}, next); err != nil {
					return err
				}
				pos = next
			}
		case *replication.GTIDEvent:
			inTransaction = true
		case *replication.TransactionPayloadEvent:
			return fmt.Errorf("compressed binlog transaction is unsupported; disable binlog_transaction_compression")
		case *replication.XIDEvent:
			if err = flush(); err != nil {
				return err
			}
			if x.GSet != nil {
				next.GTID, err = MergeGTID(pos.GTID, x.GSet)
				if err != nil {
					return err
				}
			}
			if err = p.Store.Commit(next); err != nil {
				return err
			}
			pos = next
			inTransaction = false
		case *replication.QueryEvent:
			q := strings.TrimSpace(string(x.Query))
			upper := strings.ToUpper(q)
			switch upper {
			case "BEGIN":
				inTransaction = true
				continue
			case "COMMIT":
				inTransaction = false
				if err = flush(); err != nil {
					return err
				}
				if x.GSet != nil {
					next.GTID, err = MergeGTID(pos.GTID, x.GSet)
					if err != nil {
						return err
					}
				}
				if err = p.Store.Commit(next); err != nil {
					return err
				}
				pos = next
				continue
			case "ROLLBACK":
				inTransaction = false
				pending = nil
				if err = p.Store.Rollback(); err != nil {
					return err
				}
				continue
			}
			d, parseErr := schema.Parse(q)
			database := string(x.Schema)
			references, scopeErr := schema.ReferencesDatabase(q, database, c.MySQL.Database)
			if scopeErr != nil && (database == c.MySQL.Database || references) {
				return scopeErr
			}
			if references && parseErr != nil {
				return fmt.Errorf("source DDL cannot be represented safely: %w", parseErr)
			}
			if parseErr == nil && d.Database != "" {
				database = d.Database
			}
			if references {
				database = c.MySQL.Database
			}
			if database != c.MySQL.Database {
				if x.GSet != nil {
					next.GTID, err = MergeGTID(pos.GTID, x.GSet)
					if err != nil {
						return err
					}
				}
				if err = p.Store.Append([]model.Event{{Kind: "position", Position: next, SourceID: fmt.Sprintf("safe:%s:%d", file, next.Pos)}}, next); err != nil {
					return err
				}
				pos = next
				inTransaction = false
				continue
			}
			if parseErr != nil {
				return fmt.Errorf("unhandled query event (position %s:%d): %w", file, next.Pos, parseErr)
			}
			allowed := false
			for _, name := range d.Tables {
				if p.tracked(name) {
					allowed = true
				}
			}
			if !allowed {
				if x.GSet != nil {
					next.GTID, err = MergeGTID(pos.GTID, x.GSet)
					if err != nil {
						return err
					}
				}
				if err = p.Store.Append([]model.Event{{Kind: "position", Position: next, SourceID: fmt.Sprintf("safe:%s:%d", file, next.Pos)}}, next); err != nil {
					return err
				}
				pos = next
				inTransaction = false
				continue
			}
			event := model.Event{Kind: "ddl", Table: d.Tables[0], Query: q, Position: next, SourceID: fmt.Sprintf("%s:%d", file, next.Pos)}
			var save []model.Schema
			var remove []string
			switch d.Kind {
			case "create":
				fresh, e := schema.FromCreate(q, c.MySQL.Database, journal.ID(), c)
				if e != nil {
					return e
				}
				history, err := p.Store.History()
				if err != nil {
					return err
				}
				for _, oldEvent := range history {
					if oldEvent.SourceID == event.SourceID && oldEvent.Schema != nil {
						fresh.Generation = oldEvent.Schema.Generation
					}
				}
				event.Schema = &fresh
				fresh.Revision = event.SourceID
				save = append(save, fresh)
			case "alter":
				old, e := p.Store.Schema(d.Tables[0])
				if e != nil {
					return e
				}
				fresh, rebuild, e := schema.Apply(old, q, c)
				if e != nil {
					return e
				}
				event.Previous = &old
				fresh.Revision = event.SourceID
				event.Schema = &fresh
				event.Rebuild = rebuild
				if rebuild {
					fresh.Generation = journal.ID()
					history, err := p.Store.History()
					if err != nil {
						return err
					}
					for _, oldEvent := range history {
						if oldEvent.SourceID == event.SourceID && oldEvent.Schema != nil {
							fresh.Generation = oldEvent.Schema.Generation
						}
					}
				}
				save = append(save, fresh)
			case "rename":
				for _, pair := range d.Pairs {
					if !p.tracked(pair[0]) {
						continue
					}
					old, e := p.Store.Schema(pair[0])
					if e != nil {
						return e
					}
					fresh := old
					fresh.Table = pair[1]
					fresh.Revision = event.SourceID
					save = append(save, fresh)
					remove = append(remove, pair[0])
				}
			case "drop":
				for _, table := range d.Tables {
					if p.tracked(table) {
						remove = append(remove, table)
					}
				}
			case "truncate":
				old, e := p.Store.Schema(d.Tables[0])
				if e != nil {
					return e
				}
				old.Revision = event.SourceID
				save = append(save, old)
			}
			if x.GSet != nil {
				next.GTID, err = MergeGTID(pos.GTID, x.GSet)
				if err != nil {
					return err
				}
			}
			event.Position = next
			if err = p.Store.SchemaEvent(event, save, remove); err != nil {
				return err
			}
			pos = next
			inTransaction = false
		case *replication.RowsEvent:
			if string(x.Table.Schema) != c.MySQL.Database {
				continue
			}
			table := string(x.Table.Table)
			if table == c.Migration.SignalTable {
				continue
			}
			if !p.tracked(table) {
				continue
			}
			s, err := p.Store.Schema(table)
			if err != nil {
				return fmt.Errorf("missing historical schema for %s: %w", table, err)
			}
			if len(s.Columns) != int(x.ColumnCount) {
				return fmt.Errorf("schema history mismatch for %s; resnapshot required", table)
			}
			e := model.Event{Table: table, Schema: &s, SourceID: fmt.Sprintf("%s:%d", file, next.Pos)}
			normalized := make([][]model.Value, len(x.Rows))
			for i, row := range x.Rows {
				normalized[i], err = source.NormalizeRow(s, row)
				if err != nil {
					return err
				}
			}
			switch ev.Header.EventType {
			case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				e.Kind = "insert"
				for _, r := range normalized {
					e.Rows = append(e.Rows, model.Change{After: r})
				}
			case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				e.Kind = "delete"
				for _, r := range normalized {
					e.Rows = append(e.Rows, model.Change{Before: r})
				}
			case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				e.Kind = "update"
				for i := 0; i < len(x.Rows); i += 2 {
					if i+1 >= len(x.Rows) {
						return fmt.Errorf("incomplete update image")
					}
					e.Rows = append(e.Rows, model.Change{Before: normalized[i], After: normalized[i+1]})
				}
			default:
				return fmt.Errorf("unsupported row event %s", ev.Header.EventType)
			}
			pending = append(pending, e)
			if len(pending) >= c.Migration.CDC.ProducerBatch {
				if err = flush(); err != nil {
					return err
				}
			}
		}
	}
}

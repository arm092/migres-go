package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/arm092/migres-go/internal/journal"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/pipeline"
	"github.com/arm092/migres-go/internal/schema"
	"github.com/arm092/migres-go/internal/source"
)

type rebuildState struct {
	SourceTable string
	Windows     []journal.TableWindow
	Schema      model.Schema
	Previous    model.Schema
	Shadow      model.Schema
	LastKey     []model.Value
	Start       model.Position
	Boundary    model.Position
	Step        string
}

func (a *App) Execute(ctx context.Context, t model.Task) error {
	if t.Kind == "ddl" {
		a.DDLRunning.Store(true)
		defer a.DDLRunning.Store(false)
	}
	if t.Kind == "ddl" && t.Rebuild {
		if err := a.rebuild(ctx, t); err != nil {
			return err
		}
		if t.Operation != "" {
			ops, err := a.Store.Operations()
			if err != nil {
				return err
			}
			for _, op := range ops {
				if op.ID == t.Operation {
					op.State = "done"
					op.Error = ""
					return a.Store.Operation(op)
				}
			}
			return fmt.Errorf("rebuild operation record is missing")
		}
		return nil
	}
	if err := a.Sink.Execute(ctx, t); err != nil {
		return err
	}
	if t.Kind == "insert" {
		if err := a.Store.Increment("delivered_rows", uint64(len(t.Rows))); err != nil {
			return err
		}
	}
	if a.Config.Migration.CDC.DBDebug && t.Kind != "fence" && t.Kind != "archive" {
		return a.Sink.ArchiveTask(ctx, t)
	}
	return nil
}
func (a *App) rebuild(ctx context.Context, t model.Task) error {
	a.RebuildRunning.Store(true)
	defer a.RebuildRunning.Store(false)
	a.Ready.Store(false)
	meta := "rebuild:" + t.Token
	var state rebuildState
	err := a.Store.Meta(meta, &state)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil && state.Step == "done" {
		return a.finishRebuild()
	}
	line := rebuildLineage{Table: state.SourceTable, Windows: state.Windows}
	if state.Step != "exchange" && state.Step != "retire" {
		line, err = a.rebuildLineage(ctx, t)
	}
	if err != nil {
		return err
	}
	if line.Drop != nil {
		return a.abandonDroppedRebuild(ctx, t, line, state, meta)
	}
	if state.Step != "" && state.Step != "exchange" && state.Step != "retire" && state.SourceTable != line.Table {
		return a.restartRebuild(ctx, t, state, meta)
	}
	if state.Step == "" {
		actual, err := a.Source.Schema(ctx, line.Table)
		if err != nil {
			return err
		}
		actual.Generation = journal.ID()
		actual.Revision = t.Schema.Revision
		state = rebuildState{SourceTable: line.Table, Windows: line.Windows, Schema: actual, Shadow: actual, Previous: *t.Previous, Step: "copy"}
		owned, err := a.Store.Owned()
		if err != nil {
			return err
		}
		for _, sc := range owned {
			if sc.Table == t.Table {
				state.Previous = sc
			}
		}
		state.Shadow.Table = "_migres_build_" + actual.Generation
		state.Start, err = a.Source.Position(ctx)
		if err != nil {
			return err
		}
		if err = a.Store.SetMeta(meta, state); err != nil {
			return err
		}
	}
	if state.Step == "done" {
		return nil
	}
	if state.Step == "copy" {
		window := &pipeline.Snapshot{Config: a.Config, Store: a.Store, Source: a.Source}
		if err := window.Prepare(ctx); err != nil {
			return err
		}
		if err := window.Marker(ctx, t.Table, "rebuild-open"); err != nil {
			return err
		}
		if err := a.Sink.Ensure(ctx, state.Shadow); err != nil {
			return err
		}
		for {
			rows, err := a.Source.Chunk(ctx, state.Schema, state.LastKey, a.Config.Migration.BatchRows)
			if err != nil {
				latest, e := a.rebuildLineage(ctx, t)
				if e == nil && (latest.Drop != nil || latest.Table != state.SourceTable) {
					return schema.ErrSnapshotInvalidated
				}
				return err
			}
			if len(rows) == 0 {
				break
			}
			task := model.Task{Kind: "insert", Table: state.Shadow.Table, Schema: &state.Shadow, Token: fmt.Sprintf("%s:snapshot:%v", t.Token, state.LastKey)}
			for _, row := range rows {
				task.Rows = append(task.Rows, model.Row{Values: row, Version: 1})
			}
			if err = a.Sink.Execute(ctx, task); err != nil {
				return err
			}
			state.LastKey, err = source.Key(state.Schema, rows[len(rows)-1])
			if err != nil {
				return err
			}
			if err = a.Store.SetMeta(meta, state); err != nil {
				return err
			}
		}
		if err := window.Marker(ctx, t.Table, "rebuild-close"); err != nil {
			return err
		}
		boundary, err := a.Source.Position(ctx)
		if err != nil {
			return err
		}
		state.Boundary = boundary
		state.Step = "catchup"
		if err = a.Store.SetMeta(meta, state); err != nil {
			return err
		}
	}
	if state.Step == "catchup" {
		for {
			var prepared model.Position
			a.Store.Meta("prepared_position", &prepared)
			if pipeline.Reached(prepared, state.Boundary) {
				break
			}
			stats, err := a.Store.Stats()
			if err != nil {
				return err
			}
			if stats.Events == 0 && stats.Bytes >= a.Config.MaxQueueBytes {
				return fmt.Errorf("rebuild catch-up reached max_queue_bytes; increase queue capacity before resuming")
			}
			if err = a.Store.CheckDisk(uint64(a.Config.MinFreeDiskBytes)); err != nil {
				return fmt.Errorf("rebuild catch-up needs free journal disk space: %w", err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
		}
		history, err := a.Store.History()
		if err != nil {
			return err
		}
		for _, event := range history {
			if !pipeline.Reached(state.Boundary, event.Position) || pipeline.Reached(state.Start, event.Position) {
				continue
			}
			ddl, err := schema.Parse(event.Query)
			if err != nil {
				return err
			}
			for _, name := range ddl.Tables {
				if journal.InTableWindows(state.Windows, name, event.Position) {
					return a.restartRebuild(ctx, t, state, meta)
				}
			}
		}
		startVersion, err := a.Store.SequenceAt(state.Start)
		if err != nil {
			return err
		}
		// A changed shape invalidates the copy; retrying the source DDL after recovery
		// must never publish a partially converted generation.
		current, err := a.Source.Schema(ctx, state.SourceTable)
		if err != nil {
			return err
		}
		shape := func(s model.Schema) string {
			type column struct {
				Name, Type, Charset string
				Nullable            bool
				Elements            []string
			}
			cols := []column{}
			for _, c := range s.Columns {
				cols = append(cols, column{c.Name, c.CHType, c.Charset, c.Nullable, c.Elements})
			}
			b, _ := json.Marshal(struct {
				Columns []column
				Keys    []string
			}{cols, s.Keys})
			return string(b)
		}
		if shape(current) != shape(state.Schema) {
			return a.restartRebuild(ctx, t, state, meta)
		}
		upper, err := a.Store.LastTaskID()
		if err != nil {
			return err
		}
		after := t.ID
		for after < upper {
			tasks, err := a.Store.TasksAfter(after, 100)
			if err != nil {
				return err
			}
			if len(tasks) == 0 {
				break
			}
			for _, pending := range tasks {
				after = pending.ID
				if after > upper {
					break
				}
				if !journal.InTableWindows(state.Windows, pending.Table, pending.Position) || pending.Kind != "insert" || pending.Schema == nil || shape(*pending.Schema) != shape(state.Schema) {
					continue
				}
				rows := []model.Row{}
				for _, row := range pending.Rows {
					if row.Version > startVersion {
						rows = append(rows, row)
					}
				}
				if len(rows) == 0 {
					continue
				}
				pending.Rows = rows
				pending.Table = state.Shadow.Table
				pending.Schema = &state.Shadow
				if err = a.Sink.Execute(ctx, pending); err != nil {
					return err
				}
			}
		}

		state.Step = "exchange"
		if err = a.Store.SetMeta(meta, state); err != nil {
			return err
		}
	}
	if state.Step == "exchange" {
		old := state.Previous
		if err := a.Sink.Exchange(ctx, old, state.Shadow, state.Schema); err != nil {
			return err
		}
		state.Step = "retire"
		if err := a.Store.SetMeta(meta, state); err != nil {
			return err
		}
	}
	if state.Step == "retire" {
		if err := a.Store.RetireRebuildTasks(state.Windows, state.Boundary, t.Token, true); err != nil {
			return err
		}
		state.Step = "done"
		if err := a.Store.SetMeta(meta, state); err != nil {
			return err
		}
	}
	// Future source events use the source schema, but target table ownership is the
	// published generation and is deliberately tracked separately.
	return a.finishRebuild()
}
func (a *App) finishRebuild() error {
	var snapshotDone bool
	a.Store.Meta("snapshot_done", &snapshotDone)
	if snapshotDone {
		if err := a.Store.SetMeta("resnapshot_pending", false); err != nil {
			return err
		}
		a.Ready.Store(true)
	}
	return nil
}

func (a *App) restartRebuild(ctx context.Context, t model.Task, old rebuildState, meta string) error {
	if err := a.Sink.DropOwned(ctx, old.Shadow); err != nil {
		return err
	}
	line, err := a.rebuildLineage(ctx, t)
	if err != nil {
		return err
	}
	if line.Drop != nil {
		return schema.ErrSnapshotInvalidated
	}
	actual, err := a.Source.Schema(ctx, line.Table)
	if err != nil {
		return err
	}
	actual.Generation = journal.ID()
	if current, err := a.Store.Schema(line.Table); err == nil {
		actual.Revision = current.Revision
	}
	start, err := a.Source.Position(ctx)
	if err != nil {
		return err
	}
	next := rebuildState{SourceTable: line.Table, Windows: line.Windows, Schema: actual, Previous: old.Previous, Shadow: actual, Start: start, Step: "copy"}
	next.Shadow.Table = "_migres_build_" + actual.Generation
	if err = a.Store.SetMeta(meta, next); err != nil {
		return err
	}
	return schema.ErrSnapshotInvalidated
}

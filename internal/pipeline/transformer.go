package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/arm092/migres-go/internal/journal"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/source"
)

func Transform(e model.Event) ([]model.Task, error) {
	return transform(context.Background(), e)
}

func transform(ctx context.Context, e model.Event) ([]model.Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if e.Kind == "position" {
		return nil, nil
	}
	if e.Kind == "fence" {
		return []model.Task{{Kind: "fence"}}, nil
	}
	if e.Kind == "ddl" {
		return []model.Task{{Kind: "ddl", Table: e.Table, Schema: e.Schema, Previous: e.Previous, Rebuild: e.Rebuild, SQL: e.Query, Position: e.Position, Operation: e.Operation}}, nil
	}
	if e.Schema == nil {
		return nil, fmt.Errorf("event %d lacks schema", e.Sequence)
	}
	t := model.Task{Kind: "insert", Table: e.Table, Schema: e.Schema, Position: e.Position}
	version := e.Sequence
	if e.Kind == "snapshot" {
		version = 1
	}
	for rowIndex, r := range e.Rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if e.Kind != "snapshot" {
			version = e.Sequence + uint64(rowIndex)
		}
		switch e.Kind {
		case "insert", "snapshot":
			if len(r.After) != len(e.Schema.Columns) {
				return nil, fmt.Errorf("after-image shape mismatch")
			}
			t.Rows = append(t.Rows, model.Row{Values: r.After, Version: version})
		case "delete":
			if len(r.Before) != len(e.Schema.Columns) {
				return nil, fmt.Errorf("before-image shape mismatch")
			}
			t.Rows = append(t.Rows, model.Row{Values: r.Before, Version: version, Deleted: true})
		case "update":
			if len(r.After) != len(e.Schema.Columns) || len(r.Before) != len(e.Schema.Columns) {
				return nil, fmt.Errorf("FULL before/after images required")
			}
			a, err := source.Key(*e.Schema, r.Before)
			if err != nil {
				return nil, err
			}
			b, err := source.Key(*e.Schema, r.After)
			if err != nil {
				return nil, err
			}
			aa, _ := json.Marshal(a)
			bb, _ := json.Marshal(b)
			if string(aa) != string(bb) {
				t.Rows = append(t.Rows, model.Row{Values: r.Before, Version: version, Deleted: true})
			}
			t.Rows = append(t.Rows, model.Row{Values: r.After, Version: version})
		default:
			return nil, fmt.Errorf("unsupported event kind %s", e.Kind)
		}
	}
	if len(t.Rows) == 0 {
		return nil, nil
	}
	return []model.Task{t}, nil
}

type Transformer struct {
	Store          *journal.Store
	Poll           time.Duration
	Debug          bool
	BatchRows      int
	CheckpointRows int
	MaxWait        time.Duration
}

func (t *Transformer) Run(ctx context.Context) error {
	limit := t.CheckpointRows
	if limit <= 0 {
		limit = 1000
	}
	batchRows := t.BatchRows
	if batchRows <= 0 {
		batchRows = 5000
	}
	last := time.Now()
	for {
		if ctx.Err() != nil {
			return nil
		}
		events, err := t.Store.Events(limit)
		if err != nil {
			return err
		}
		shouldWait := len(events) == 0 || (t.CheckpointRows > 0 && len(events) < limit && time.Since(last) < t.MaxWait)
		if shouldWait {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(t.Poll):
			}
			continue
		}
		all := []model.Task{}
		ids := []uint64{}
		for _, e := range events {
			tasks, err := transform(ctx, e)
			if ctx.Err() != nil {
				return nil
			}
			if err != nil {
				return err
			}
			ids = append(ids, e.Sequence)
			if t.Debug {
				payload, err := json.Marshal(e)
				if err != nil {
					return err
				}
				all = append(all, model.Task{Kind: "archive", Table: "_migres_debug_events", Payload: string(payload), Rows: []model.Row{{Version: e.Sequence}}})
			}
			for _, task := range tasks {
				for task.Kind == "insert" && len(task.Rows) > batchRows {
					if ctx.Err() != nil {
						return nil
					}
					part := task
					part.Rows = append([]model.Row(nil), task.Rows[:batchRows]...)
					all = append(all, part)
					task.Rows = task.Rows[batchRows:]
				}
				if task.Kind == "insert" && len(all) > 0 {
					prev := &all[len(all)-1]
					if prev.Kind == "insert" && prev.Table == task.Table && prev.Schema.Revision == task.Schema.Revision && prev.Schema.Generation == task.Schema.Generation && len(prev.Rows)+len(task.Rows) <= batchRows {
						prev.Rows = append(prev.Rows, task.Rows...)
						prev.Position = task.Position
						continue
					}
				}
				all = append(all, task)
			}
		}
		if ctx.Err() != nil {
			return nil
		}
		if err = t.Store.TransformBatch(ids, all); err != nil {
			return err
		}
		last = time.Now()
	}
}

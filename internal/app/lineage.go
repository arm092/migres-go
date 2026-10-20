// Rebuild lineage is resolved from committed schema history, never a guess based
// on current table names. A pending rename remains a target-side DDL barrier.
package app

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/arm092/migres-go/internal/journal"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/pipeline"
	"github.com/arm092/migres-go/internal/schema"
)

type rebuildLineage struct {
	Table   string
	Windows []journal.TableWindow
	Drop    *model.Event
}

func (a *App) rebuildLineage(ctx context.Context, t model.Task) (rebuildLineage, error) {
	out := rebuildLineage{Table: t.Table, Windows: []journal.TableWindow{{Table: t.Table, After: t.Position}}}
	boundary, err := a.Source.Position(ctx)
	if err != nil {
		return out, err
	}
	for {
		st, err := a.Store.Stats()
		if err != nil {
			return out, err
		}
		if pipeline.Reached(st.Checkpoint, boundary) && st.Events == 0 {
			break
		}
		var prepared model.Position
		a.Store.Meta("prepared_position", &prepared)
		if pipeline.Reached(prepared, boundary) {
			break
		}
		if st.Events == 0 && st.Bytes >= a.Config.MaxQueueBytes {
			return out, fmt.Errorf("rebuild lineage reached max_queue_bytes; increase queue capacity")
		}
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	history, err := a.Store.History()
	if err != nil {
		return out, err
	}
	for _, e := range history {
		if pipeline.Reached(t.Position, e.Position) || !pipeline.Reached(boundary, e.Position) {
			continue
		}
		ddl, err := schema.Parse(e.Query)
		if err != nil {
			return out, err
		}
		if len(ddl.Pairs) > 0 {
			for _, pair := range ddl.Pairs {
				if pair[0] == out.Table {
					out.Windows[len(out.Windows)-1].Through = e.Position
					out.Table = pair[1]
					out.Windows = append(out.Windows, journal.TableWindow{Table: out.Table, After: e.Position})
				}
			}
		}
		if ddl.Kind == "drop" && slices.Contains(ddl.Tables, out.Table) {
			out.Windows[len(out.Windows)-1].Through = e.Position
			copy := e
			out.Drop = &copy
			break
		}
	}
	return out, nil
}

// A later DROP makes copying obsolete. Cleanup is idempotent and happens before
// retiring its queued history, so a crash cannot acknowledge an unperformed DROP.
func (a *App) abandonDroppedRebuild(ctx context.Context, t model.Task, line rebuildLineage, state rebuildState, meta string) error {
	owned, err := a.Store.Owned()
	if err != nil {
		return err
	}
	for _, s := range owned {
		if s.Table == state.Shadow.Table {
			if err = a.Sink.DropOwned(ctx, s); err != nil {
				return err
			}
		}
	}
	if err = a.Store.RetireRebuildTasks(line.Windows, line.Drop.Position, t.Token, true); err != nil {
		return err
	}
	state.Step = "done"
	if err := a.Store.SetMeta(meta, state); err != nil {
		return err
	}
	return a.finishRebuild()
}

package journal

import (
	"encoding/json"
	"fmt"
	"github.com/arm092/migres-go/internal/schema"

	"github.com/arm092/migres-go/internal/model"
	gm "github.com/go-mysql-org/go-mysql/mysql"
	bolt "go.etcd.io/bbolt"
)

func positionLE(a, b model.Position) bool {
	if a.File == "" {
		return true
	}
	return (gm.Position{Name: a.File, Pos: a.Pos}).Compare(gm.Position{Name: b.File, Pos: b.Pos}) <= 0
}

// RetireTableTasks is called only after all replicas expose the new generation.
func (s *Store) RetireTableTasks(table string, boundary model.Position, operation string) error {
	return s.RetireRebuildTasks([]TableWindow{{Table: table}}, boundary, operation, false)
}

type TableWindow struct {
	Table   string
	After   model.Position
	Through model.Position
}

func InTableWindows(windows []TableWindow, table string, p model.Position) bool {
	for _, w := range windows {
		if w.Table == table && positionLE(w.After, p) && (w.Through.File == "" || positionLE(p, w.Through)) {
			return true
		}
	}
	return false
}
func (s *Store) RetireRebuildTasks(windows []TableWindow, boundary model.Position, operation string, keepRenames bool) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("tasks"))
		archive := tx.Bucket([]byte("archive"))
		cur := b.Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			var t model.Task
			if err := json.Unmarshal(v, &t); err != nil {
				return err
			}
			// The coordinator must remain replayable until its state and operation
			// completion are durable. Consumer acknowledges it afterward.
			if keepRenames && t.Kind == "ddl" && t.SQL != "" {
				d, err := schema.Parse(t.SQL)
				if err != nil {
					return err
				}
				if d.Kind == "alter" && len(d.Pairs) > 0 && t.Token != operation && InTableWindows(windows, t.Table, t.Position) && positionLE(t.Position, boundary) {
					// The new generation already contains this ALTER's final shape.
					// Keep its naming barrier, but do not replay destructive column
					// steps (DROP/ADD name reuse) on the rebuilt data.
					if err := put(tx.Bucket([]byte("meta")), []byte("compoundabsorbed:"+t.Token), true); err != nil {
						return err
					}
				}
				if len(d.Pairs) > 0 || d.Kind == "drop" {
					continue
				}
			}
			// Multi-table DDL may also affect a different lineage; leave it for
			// normal ordered delivery, including a DROP already done for this one.
			if t.Kind == "ddl" && t.SQL != "" && t.Token != operation {
				d, err := schema.Parse(t.SQL)
				if err != nil {
					return err
				}
				if len(d.Tables) > 1 {
					continue
				}
			}
			if InTableWindows(windows, t.Table, t.Position) && t.Token != operation && t.Operation == "" && positionLE(t.Position, boundary) {
				if err := archive.Put([]byte(fmt.Sprintf("cutover/%s/%x", operation, k)), v); err != nil {
					return err
				}
				if err := cur.Delete(); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

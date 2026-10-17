package journal

import (
	"encoding/json"
	"fmt"

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
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("tasks"))
		archive := tx.Bucket([]byte("archive"))
		cur := b.Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			var t model.Task
			if err := json.Unmarshal(v, &t); err != nil {
				return err
			}
			if t.Table == table && positionLE(t.Position, boundary) {
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

package journal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/arm092/migres-go/internal/model"
	bolt "go.etcd.io/bbolt"
)

func (s *Store) TasksAfter(after uint64, limit int) ([]model.Task, error) {
	out := []model.Task{}
	err := s.db.View(func(tx *bolt.Tx) error {
		cur := tx.Bucket([]byte("tasks")).Cursor()
		for k, v := cur.Seek(key(after + 1)); k != nil && len(out) < limit; k, v = cur.Next() {
			var t model.Task
			if err := json.Unmarshal(v, &t); err != nil {
				return err
			}
			out = append(out, t)
		}
		return nil
	})
	return out, err
}
func (s *Store) SequenceAt(p model.Position) (uint64, error) {
	var n uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		for _, id := range []string{fmt.Sprintf("commit:%s:%d", p.File, p.Pos), fmt.Sprintf("%s:%d", p.File, p.Pos), fmt.Sprintf("safe:%s:%d", p.File, p.Pos)} {
			if v := tx.Bucket([]byte("versions")).Get([]byte(id)); v != nil {
				n = binary.BigEndian.Uint64(v) + 1
				return nil
			}
		}
		var initial model.Position
		if v := tx.Bucket([]byte("meta")).Get([]byte("initial_position")); v != nil {
			if err := json.Unmarshal(v, &initial); err != nil {
				return err
			}
			if initial.File == p.File && initial.Pos == p.Pos {
				n = 1
				return nil
			}
		}
		return fmt.Errorf("no retained source boundary version")
	})
	return n, err
}

func (s *Store) LastTaskID() (uint64, error) {
	var n uint64
	err := s.db.View(func(tx *bolt.Tx) error { n = tx.Bucket([]byte("tasks")).Sequence(); return nil })
	return n, err
}

package journal

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/arm092/migres-go/internal/model"
	bolt "go.etcd.io/bbolt"
)

func (s *Store) Reset() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, name := range buckets {
			if name == "operations" {
				continue
			}
			if err := tx.DeleteBucket([]byte(name)); err != nil {
				return err
			}
			if _, err := tx.CreateBucket([]byte(name)); err != nil {
				return err
			}
		}
		return nil
	})
}
func (s *Store) DiscardPending(operation string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		archive := tx.Bucket([]byte("archive"))
		for _, name := range []string{"events", "tasks", "transactions"} {
			b := tx.Bucket([]byte(name))
			if err := b.ForEach(func(k, v []byte) error { return archive.Put([]byte(fmt.Sprintf("%s/%s/%x", operation, name, k)), v) }); err != nil {
				return err
			}
			if err := tx.DeleteBucket([]byte(name)); err != nil {
				return err
			}
			if _, err := tx.CreateBucket([]byte(name)); err != nil {
				return err
			}
		}
		return nil
	})
}
func (s *Store) Reposition(p model.Position, schemas []model.Schema, gap bool) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte("schemas")); err != nil {
			return err
		}
		b, err := tx.CreateBucket([]byte("schemas"))
		if err != nil {
			return err
		}
		for _, sc := range schemas {
			if err = put(b, []byte(sc.Table), sc); err != nil {
				return err
			}
		}
		if gap {
			if err = put(tx.Bucket([]byte("meta")), []byte("completeness_lost"), true); err != nil {
				return err
			}
		}
		return put(tx.Bucket([]byte("meta")), []byte("checkpoint"), p)
	})
}
func (s *Store) History() ([]model.Event, error) { return read[model.Event](s, "schema_history", 0) }
func (s *Store) ArchiveTask(t model.Task) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return put(tx.Bucket([]byte("archive")), []byte(fmt.Sprintf("debug/task/%020d", t.ID)), struct {
			At   time.Time
			Task model.Task
		}{time.Now().UTC(), t})
	})
}
func (s *Store) ArchiveEvent(e model.Event) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return put(tx.Bucket([]byte("archive")), []byte(fmt.Sprintf("debug/event/%020d", e.Sequence)), struct {
			At    time.Time
			Event model.Event
		}{time.Now().UTC(), e})
	})
}
func (s *Store) PruneDebug(before time.Time) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("archive"))
		cur := b.Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			if len(k) < 6 || string(k[:6]) != "debug/" {
				continue
			}
			var item struct{ At time.Time }
			if err := json.Unmarshal(v, &item); err != nil {
				return err
			}
			if item.At.Before(before) {
				if err := cur.Delete(); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

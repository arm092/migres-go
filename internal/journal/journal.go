// Package journal owns the single-process durable stage boundaries.
package journal

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/arm092/migres-go/internal/model"
	bolt "go.etcd.io/bbolt"
)

var buckets = []string{"events", "tasks", "meta", "schemas", "progress", "operations", "archive", "transactions", "versions", "schema_history", "owned", "boundaries"}

type Store struct {
	db                  *bolt.DB
	path                string
	MaxTransactionBytes int64
}

var ErrSchemaChanged = errors.New("snapshot schema changed while reading chunk")

func Open(path string, timeout time.Duration) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: timeout})
	if err != nil {
		return nil, err
	}
	s := &Store{db: db, path: path, MaxTransactionBytes: 1 << 30}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range buckets {
			if _, err := tx.CreateBucketIfNotExists([]byte(b)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}
func (s *Store) Close() error { return s.db.Close() }
func key(n uint64) []byte     { b := make([]byte, 8); binary.BigEndian.PutUint64(b, n); return b }
func put(b *bolt.Bucket, k []byte, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return b.Put(k, data)
}
func ID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func (s *Store) Meta(name string, out any) error {
	return s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte("meta")).Get([]byte(name))
		if v == nil {
			return os.ErrNotExist
		}
		return json.Unmarshal(v, out)
	})
}
func (s *Store) SetMeta(name string, v any) error {
	return s.db.Update(func(tx *bolt.Tx) error { return put(tx.Bucket([]byte("meta")), []byte(name), v) })
}
func appendEvents(tx *bolt.Tx, events []model.Event, pos *model.Position) error {
	b := tx.Bucket([]byte("events"))
	m := tx.Bucket([]byte("meta"))
	for _, e := range events {
		var n uint64
		var err error
		versions := tx.Bucket([]byte("versions"))
		if v := versions.Get([]byte(e.SourceID)); e.SourceID != "" && v != nil {
			n = binary.BigEndian.Uint64(v)
		} else {
			n, err = m.NextSequence()
			if err != nil {
				return err
			}
			if err = m.SetSequence(n + uint64(len(e.Rows))); err != nil {
				return err
			}
			if e.SourceID != "" {
				if err = versions.Put([]byte(e.SourceID), key(n)); err != nil {
					return err
				}
			}
		}
		e.Sequence = n + 1 // version 1 is reserved for snapshot baseline
		if err = put(b, key(e.Sequence), e); err != nil {
			return err
		}
	}
	if pos != nil {
		if err := put(tx.Bucket([]byte("boundaries")), []byte(fmt.Sprintf("%s:%010d", pos.File, pos.Pos)), pos); err != nil {
			return err
		}
		return put(m, []byte("checkpoint"), pos)
	}
	return nil
}
func (s *Store) Append(events []model.Event, pos model.Position) error {
	return s.db.Update(func(tx *bolt.Tx) error { return appendEvents(tx, events, &pos) })
}
func (s *Store) Enqueue(events []model.Event) error {
	return s.db.Update(func(tx *bolt.Tx) error { return appendEvents(tx, events, nil) })
}
func (s *Store) SnapshotRows(e model.Event, p model.Progress) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		var current model.Schema
		raw := tx.Bucket([]byte("schemas")).Get([]byte(e.Table))
		if raw == nil {
			return ErrSchemaChanged
		}
		if err := json.Unmarshal(raw, &current); err != nil {
			return err
		}
		if e.Schema == nil || current.Revision != e.Schema.Revision || current.Generation != e.Schema.Generation {
			return ErrSchemaChanged
		}
		if err := appendEvents(tx, []model.Event{e}, nil); err != nil {
			return err
		}
		return put(tx.Bucket([]byte("progress")), []byte(p.Table), p)
	})
}
func (s *Store) Stage(e model.Event) error {
	return s.StageMany([]model.Event{e})
}
func (s *Store) StageMany(events []model.Event) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("transactions"))
		bs := b.Stats()
		total := int64(bs.LeafInuse)
		for _, e := range events {
			data, err := json.Marshal(e)
			if err != nil {
				return err
			}
			total += int64(len(data))
			if total > s.MaxTransactionBytes {
				return fmt.Errorf("transaction exceeds durable staging limit; raise max_transaction_bytes before retry")
			}
			n, err := b.NextSequence()
			if err != nil {
				return err
			}
			if err = b.Put(key(n), data); err != nil {
				return err
			}
		}
		return nil
	})
}
func (s *Store) Commit(pos model.Position) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("transactions"))
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var e model.Event
			if err := json.Unmarshal(v, &e); err != nil {
				return err
			}
			e.Position = pos
			if err := appendEvents(tx, []model.Event{e}, nil); err != nil {
				return err
			}
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return appendEvents(tx, []model.Event{{Kind: "position", Position: pos, SourceID: fmt.Sprintf("commit:%s:%d", pos.File, pos.Pos)}}, &pos)
	})
}
func (s *Store) Rollback() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte("transactions")); err != nil {
			return err
		}
		_, err := tx.CreateBucket([]byte("transactions"))
		return err
	})
}
func read[T any](s *Store, bucket string, limit int) ([]T, error) {
	out := []T{}
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(bucket)).Cursor()
		for k, v := c.First(); k != nil && (limit <= 0 || len(out) < limit); k, v = c.Next() {
			var item T
			if err := json.Unmarshal(v, &item); err != nil {
				return err
			}
			out = append(out, item)
		}
		return nil
	})
	return out, err
}
func (s *Store) Events(limit int) ([]model.Event, error) {
	return read[model.Event](s, "events", limit)
}
func (s *Store) Tasks(limit int) ([]model.Task, error) { return read[model.Task](s, "tasks", limit) }
func (s *Store) Transform(sequence uint64, tasks []model.Task) error {
	return s.TransformBatch([]uint64{sequence}, tasks)
}
func (s *Store) TransformBatch(ids []uint64, tasks []model.Task) error {
	if len(ids) == 0 {
		return nil
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		events := tx.Bucket([]byte("events"))
		for _, id := range ids {
			v := events.Get(key(id))
			if v == nil {
				return errors.New("event no longer pending")
			}
			var original model.Event
			if err := json.Unmarshal(v, &original); err != nil {
				return err
			}
			if original.Position.File != "" && original.Kind != "snapshot" {
				if err := put(tx.Bucket([]byte("meta")), []byte("prepared_position"), original.Position); err != nil {
					return err
				}
			}
		}
		b := tx.Bucket([]byte("tasks"))
		for _, t := range tasks {
			n, err := b.NextSequence()
			if err != nil {
				return err
			}
			t.ID = n
			t.Token = fmt.Sprintf("migres:%d:%d", ids[0], n)
			if err = put(b, key(n), t); err != nil {
				return err
			}
		}
		for _, id := range ids {
			if err := events.Delete(key(id)); err != nil {
				return err
			}
		}
		return nil
	})
}
func (s *Store) Ack(id uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("tasks")).Delete(key(id)) })
}
func (s *Store) Schema(name string) (model.Schema, error) {
	var out model.Schema
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("schemas"))
		v := b.Get([]byte(name))
		if v == nil {
			return os.ErrNotExist
		}
		return json.Unmarshal(v, &out)
	})
	return out, err
}
func (s *Store) SaveSchema(v model.Schema) error {
	return s.db.Update(func(tx *bolt.Tx) error { return put(tx.Bucket([]byte("schemas")), []byte(v.Table), v) })
}
func (s *Store) SchemaEvent(e model.Event, schemas []model.Schema, remove []string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("schemas"))
		for _, name := range remove {
			if err := b.Delete([]byte(name)); err != nil {
				return err
			}
		}
		for _, sc := range schemas {
			if err := put(b, []byte(sc.Table), sc); err != nil {
				return err
			}
		}
		h := tx.Bucket([]byte("schema_history"))
		duplicate := false
		if err := h.ForEach(func(k, v []byte) error {
			var old model.Event
			if err := json.Unmarshal(v, &old); err != nil {
				return err
			}
			if old.SourceID == e.SourceID && e.SourceID != "" {
				duplicate = true
			}
			return nil
		}); err != nil {
			return err
		}
		if duplicate {
			return appendEvents(tx, []model.Event{e}, &e.Position)
		}
		n, err := h.NextSequence()
		if err != nil {
			return err
		}
		if err = put(h, key(n), e); err != nil {
			return err
		}
		return appendEvents(tx, []model.Event{e}, &e.Position)
	})
}
func (s *Store) Schemas() ([]model.Schema, error) { return read[model.Schema](s, "schemas", 0) }
func (s *Store) Owned() ([]model.Schema, error)   { return read[model.Schema](s, "owned", 0) }
func (s *Store) Own(sc model.Schema) error {
	return s.db.Update(func(tx *bolt.Tx) error { return put(tx.Bucket([]byte("owned")), []byte(sc.Table), sc) })
}
func (s *Store) Disown(table string) error {
	return s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("owned")).Delete([]byte(table)) })
}
func (s *Store) Progress(table string) (model.Progress, error) {
	var p model.Progress
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte("progress")).Get([]byte(table))
		if v == nil {
			return os.ErrNotExist
		}
		return json.Unmarshal(v, &p)
	})
	return p, err
}
func (s *Store) SetProgress(p model.Progress) error {
	return s.db.Update(func(tx *bolt.Tx) error { return put(tx.Bucket([]byte("progress")), []byte(p.Table), p) })
}
func (s *Store) Stats() (model.Stats, error) {
	var st model.Stats
	err := s.db.View(func(tx *bolt.Tx) error {
		st.Events = tx.Bucket([]byte("events")).Stats().KeyN
		st.Tasks = tx.Bucket([]byte("tasks")).Stats().KeyN
		st.StagedEvents = tx.Bucket([]byte("transactions")).Stats().KeyN
		oldest := int64(0)
		if _, v := tx.Bucket([]byte("events")).Cursor().First(); v != nil {
			var e model.Event
			if err := json.Unmarshal(v, &e); err != nil {
				return err
			}
			oldest = e.Position.Timestamp
		}
		if _, v := tx.Bucket([]byte("tasks")).Cursor().First(); v != nil {
			var t model.Task
			if err := json.Unmarshal(v, &t); err != nil {
				return err
			}
			ts := t.Position.Timestamp
			if ts > 0 && (oldest == 0 || ts < oldest) {
				oldest = ts
			}
		}
		if oldest > 0 {
			st.OldestAgeSeconds = max(0, time.Now().Unix()-oldest)
		}
		for _, name := range []string{"events", "tasks"} {
			bs := tx.Bucket([]byte(name)).Stats()
			st.Bytes += int64(bs.LeafInuse + bs.BranchInuse)
		}
		v := tx.Bucket([]byte("meta")).Get([]byte("checkpoint"))
		if v != nil {
			return json.Unmarshal(v, &st.Checkpoint)
		}
		return nil
	})
	st.ReusableBytes = int64(s.db.Stats().FreeAlloc)
	if fi, err := os.Stat(s.path); err == nil {
		st.JournalBytes = fi.Size()
	}
	return st, err
}

// StorageStats scans retained history for diagnostics only. Keep this out of the
// Producer/backpressure hot path: history grows even when active queues drain.
func (s *Store) StorageStats() (model.Stats, error) {
	st, err := s.Stats()
	if err != nil {
		return st, err
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		for _, name := range []string{"versions", "boundaries", "schema_history", "operations", "archive"} {
			bs := tx.Bucket([]byte(name)).Stats()
			st.HistoryBytes += int64(bs.LeafInuse + bs.BranchInuse)
			st.HistoryEntries += bs.KeyN
		}
		return nil
	})
	return st, err
}
func (s *Store) Operation(op model.Operation) error {
	return s.db.Update(func(tx *bolt.Tx) error { return put(tx.Bucket([]byte("operations")), []byte(op.ID), op) })
}
func (s *Store) Operations() ([]model.Operation, error) {
	return read[model.Operation](s, "operations", 0)
}

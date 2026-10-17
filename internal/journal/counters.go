package journal

import (
	"encoding/json"

	bolt "go.etcd.io/bbolt"
)

func (s *Store) Increment(name string, n uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("meta"))
		var value uint64
		if v := b.Get([]byte(name)); v != nil {
			if err := json.Unmarshal(v, &value); err != nil {
				return err
			}
		}
		return put(b, []byte(name), value+n)
	})
}

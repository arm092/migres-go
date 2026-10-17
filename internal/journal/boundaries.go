package journal

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/arm092/migres-go/internal/model"
	gm "github.com/go-mysql-org/go-mysql/mysql"
	bolt "go.etcd.io/bbolt"
)

func (s *Store) Boundaries() ([]model.Position, error) {
	return read[model.Position](s, "boundaries", 0)
}

func (s *Store) Boundary(file string, pos uint32) (model.Position, error) {
	var p model.Position
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte("boundaries")).Get([]byte(fmt.Sprintf("%s:%010d", file, pos)))
		if v == nil {
			return os.ErrNotExist
		}
		return json.Unmarshal(v, &p)
	})
	return p, err
}

// Scan one record at a time; control commands must not load a long-running
// instance's entire boundary history into memory.
func (s *Store) GTIDBoundary(raw string) (model.Position, error) {
	var found model.Position
	set, err := gm.ParseGTIDSet("mysql", raw)
	if err != nil {
		return found, err
	}
	err = s.db.View(func(tx *bolt.Tx) error {
		cur := tx.Bucket([]byte("boundaries")).Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			var p model.Position
			if e := json.Unmarshal(v, &p); e != nil {
				return e
			}
			if p.GTID == "" {
				continue
			}
			known, e := gm.ParseGTIDSet("mysql", p.GTID)
			if e != nil {
				return e
			}
			if known.Equal(set) {
				found = p
				return nil
			}
		}
		return os.ErrNotExist
	})
	return found, err
}

func (s *Store) RememberBoundary(p model.Position) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return put(tx.Bucket([]byte("boundaries")), []byte(fmt.Sprintf("%s:%010d", p.File, p.Pos)), p)
	})
}

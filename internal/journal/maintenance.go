package journal

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ExportOffline requires exclusive ownership of an existing journal. It publishes
// a complete, checked snapshot without replacing an existing destination. Compact
// copies live pages only; replay history is deliberately retained.
func ExportOffline(source, destination string, compact bool) error {
	if destination == "" {
		return fmt.Errorf("output path is required")
	}
	info, err := os.Stat(source)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("journal must be a regular file")
	}
	db, err := bolt.Open(source, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return fmt.Errorf("open journal exclusively (stop Migres first): %w", err)
	}
	defer db.Close()
	if err = checkJournal(db); err != nil {
		return err
	}
	if _, err = os.Lstat(destination); !os.IsNotExist(err) {
		return fmt.Errorf("destination must not exist")
	}
	f, err := os.CreateTemp(filepath.Dir(destination), ".migres-journal-*")
	if err != nil {
		return err
	}
	temporary := f.Name()
	defer os.Remove(temporary)
	if err = f.Close(); err != nil {
		return err
	}
	if compact {
		out, openErr := bolt.Open(temporary, 0600, nil)
		if openErr != nil {
			return openErr
		}
		err = bolt.Compact(out, db, 16<<20)
		if err == nil {
			err = checkJournal(out)
		}
		closeErr := out.Close()
		if err == nil {
			err = closeErr
		}
	} else {
		err = db.View(func(tx *bolt.Tx) error { return tx.CopyFile(temporary, 0600) })
	}
	if err != nil {
		return err
	}
	f, err = os.OpenFile(temporary, os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	// A hard link publishes atomically and fails if another process created the
	// destination. Keep temporary and destination on the same filesystem.
	if err = os.Link(temporary, destination); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(destination))
}

func checkJournal(db *bolt.DB) error {
	return db.View(func(tx *bolt.Tx) error {
		for _, name := range buckets {
			if tx.Bucket([]byte(name)) == nil {
				return fmt.Errorf("invalid journal: missing bucket %s", name)
			}
		}
		var first error
		for err := range tx.Check() {
			if first == nil {
				first = err
			}
		}
		return first
	})
}

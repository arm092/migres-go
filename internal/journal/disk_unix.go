//go:build !windows

package journal

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func (s *Store) CheckDisk(reserve uint64) error {
	var st unix.Statfs_t
	if err := unix.Statfs(s.path, &st); err != nil {
		return err
	}
	if st.Bavail*uint64(st.Bsize) < reserve {
		return fmt.Errorf("journal disk free space below reserve; delivery paused")
	}
	return nil
}

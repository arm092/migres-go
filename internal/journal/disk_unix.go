//go:build !windows

package journal

import (
	"fmt"

	"golang.org/x/sys/unix"
)

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

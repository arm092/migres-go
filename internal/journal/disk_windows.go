//go:build windows

package journal

import (
	"fmt"
	"path/filepath"

	"golang.org/x/sys/windows"
)

// Linux is the deployment target. Windows does not support directory fsync.
func syncDirectory(string) error { return nil }

func (s *Store) CheckDisk(reserve uint64) error {
	path, err := windows.UTF16PtrFromString(filepath.Dir(s.path))
	if err != nil {
		return err
	}
	var free, total, totalFree uint64
	if err = windows.GetDiskFreeSpaceEx(path, &free, &total, &totalFree); err != nil {
		return err
	}
	if free < reserve {
		return fmt.Errorf("journal disk free space below reserve; delivery paused")
	}
	return nil
}

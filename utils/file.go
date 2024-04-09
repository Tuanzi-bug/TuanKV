package utils

import (
	"github.com/shirou/gopsutil/v3/disk"
	"io/fs"
	"path/filepath"
	"syscall"
)

func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func AvailableDiskSize() (uint64, error) {
	wd, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}
	info, err := disk.Usage(wd)
	if err != nil {
		return 0, err
	}
	return info.Used, err
}

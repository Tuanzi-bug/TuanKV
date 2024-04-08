package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

type MMap struct {
	readerAt *mmap.ReaderAt
}

func NewMMapIOManager(filename string) (*MMap, error) {
	f, err := os.OpenFile(filename, os.O_CREATE, DataFilePerm)
	if err := f.Close(); err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(filename)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

func (m *MMap) Read(b []byte, offset int64) (int, error) {
	return m.readerAt.ReadAt(b, offset)

}
func (m *MMap) Write([]byte) (int, error) {
	panic("not implemented")
}

// Sync can persist data to the disk
func (m *MMap) Sync() error {
	panic("not implemented")
}
func (m *MMap) Close() error {
	return m.readerAt.Close()
}
func (m *MMap) Size() (int64, error) {
	return int64(m.readerAt.Len()), nil
}

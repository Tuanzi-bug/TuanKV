package data

import "bitcask-go/fio"

type DataFile struct {
	FileId    uint32
	WriteOff  int64
	IoManager fio.IOManager
}

func OpenDataFile(dirpath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) Read(offset int64) ([]byte, error) {
	return nil, nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) Sync() error {
	return nil
}

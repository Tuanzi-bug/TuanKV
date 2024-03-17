package bitcask_go

import (
	"bitcask-go/index"
	"errors"
)

type Options struct {
	DirPath string // Database file address

	DataFileSize int64

	SyncWrites bool

	IndexType index.IndexType
}

func checkOptions(options Options) error {
	if len(options.DirPath) == 0 {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}

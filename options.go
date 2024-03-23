package bitcask_go

import (
	"bitcask-go/index"
	"errors"
	"path"
)

type Options struct {
	DirPath string // Database file address

	DataFileSize int64

	SyncWrites bool

	IndexType index.IndexType
}

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
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

var DefaultOptions = Options{
	DirPath:      path.Join("../tem"),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	IndexType:    index.Btree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

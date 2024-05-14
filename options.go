package bitcask_go

import (
	"errors"
	"github.com/Tuanzi-bug/TuanKV/index"
	"path"
)

type Options struct {
	DirPath string // Database file address

	DataFileSize int64

	SyncWrites bool

	IndexType index.IndexType

	BytesPerSync uint

	MMapAtStartup bool

	DataFileMergeRatio float32
}

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

type WriteBatchOptions struct {
	MaxBatchNum int
	SyncWrites  bool
}

func checkOptions(options Options) error {
	if len(options.DirPath) == 0 {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("invalid merge ratio, must between 0 and 1")
	}
	return nil
}

var DefaultOptions = Options{
	DirPath:            path.Join("../tem"),
	DataFileSize:       256 * 1024 * 1024, // 256MB
	SyncWrites:         false,
	IndexType:          index.Btree,
	BytesPerSync:       0,
	MMapAtStartup:      false,
	DataFileMergeRatio: 0.5,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  false,
}

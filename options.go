package bitcask_go

type Options struct {
	DirPath string // Database file address

	DataFileSize int64

	SyncWrites bool
}

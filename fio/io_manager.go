package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	StandardFIO FileIOType = iota

	MemoryMap
)

// IOManager is an interface that represents the file I/O operations.
// Currently only standard file IO is supported
type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	// Sync can persist data to the disk
	Sync() error
	Close() error
	Size() (int64, error)
}

func NewIOManager(filename string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(filename)
	case MemoryMap:
		return NewMMapIOManager(filename)
	default:
		panic("unsupported io type")
	}
}

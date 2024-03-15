package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// LogRecord is a struct that represents the data record on the disk.
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos is a struct that represents the position of data record on the disk.
type LogRecordPos struct {
	Fid    uint32 // File ID : represents that the file in which the data will be stored.
	Offset int64  // offset in the file : represents where the data will be stored in the data file.
}

func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0
}

func (lr *LogRecord) ReadLogRecord(offset int64) (*LogRecord, error) {
	return nil, nil
}

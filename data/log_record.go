package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// crc + type+keySize+valueSize= 4+1+5+5
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// LogRecord is a struct that represents the data record on the disk.
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type LogRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
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

func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}

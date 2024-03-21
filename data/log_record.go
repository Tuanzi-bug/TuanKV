package data

import (
	"encoding/binary"
	"hash/crc32"
)

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
	header := make([]byte, maxLogRecordHeaderSize)

	var index = 4
	header[index] = logRecord.Type
	index += 1
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	recordSize := index + len(logRecord.Key) + len(logRecord.Value)

	encBytes := make([]byte, recordSize)

	copy(encBytes[:index], header[:index])
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)
	return encBytes, int64(recordSize)
}

func (lr *LogRecord) ReadLogRecord(offset int64) (*LogRecord, error) {
	return nil, nil
}

func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) < 4 {
		return nil, 0
	}
	lgHeader := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}
	var index = 5
	keySize, n := binary.Varint(buf[index:])
	lgHeader.keySize = uint32(keySize)
	index += n
	valueSize, n := binary.Varint(buf[index:])
	lgHeader.valueSize = uint32(valueSize)
	index += n
	return lgHeader, int64(index)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(header)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}

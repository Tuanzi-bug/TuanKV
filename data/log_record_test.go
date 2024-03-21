package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	// 正确情况
	lr1 := &LogRecord{
		Key:   []byte("tuan"),
		Value: []byte("t"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(lr1)
	t.Log(res1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))

	// value 为空
	lr2 := &LogRecord{
		Key:  []byte("tuan"),
		Type: LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(lr2)
	t.Log(res2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))

	// 对 deleted 情况
	lr3 := &LogRecord{
		Key:   []byte("tuan"),
		Value: []byte("tuan"),
		Type:  LogRecordDeleted,
	}
	res3, n3 := EncodeLogRecord(lr3)
	t.Log(res3)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
}

func TestDecodeLogRecord(t *testing.T) {
	headerBuf1 := []byte{252, 173, 227, 208, 0, 8, 8}
	des1, size1 := decodeLogRecordHeader(headerBuf1)
	t.Log(des1)
	assert.NotNil(t, des1)
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, uint32(3504582140), des1.crc)
	assert.Equal(t, LogRecordNormal, des1.recordType)
	assert.Equal(t, uint32(4), des1.keySize)
	assert.Equal(t, uint32(4), des1.valueSize)

	// value 为空
	headerBuf2 := []byte{218, 214, 180, 17, 0, 8, 0}
	des2, size2 := decodeLogRecordHeader(headerBuf2)
	t.Log(des2)
	assert.NotNil(t, des2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(297064154), des2.crc)
	assert.Equal(t, LogRecordNormal, des2.recordType)
	assert.Equal(t, uint32(4), des2.keySize)
	assert.Equal(t, uint32(0), des2.valueSize)

	// 对 deleted 情况
	headerBuf3 := []byte{60, 114, 109, 17, 1, 8, 8}
	des3, size3 := decodeLogRecordHeader(headerBuf3)
	t.Log(des3)
	assert.NotNil(t, des3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(292385340), des3.crc)
	assert.Equal(t, LogRecordDeleted, des3.recordType)
	assert.Equal(t, uint32(4), des3.keySize)
	assert.Equal(t, uint32(4), des3.valueSize)
}

func TestGetLogRecordCRC(t *testing.T) {
	lr1 := &LogRecord{
		Key:   []byte("tuan"),
		Value: []byte("tuan"),
		Type:  LogRecordNormal,
	}
	res1, _ := EncodeLogRecord(lr1)
	headerBuf1 := res1[:7]

	crc1 := getLogRecordCRC(lr1, headerBuf1[crc32.Size:])
	assert.Equal(t, uint32(3504582140), crc1)

	// value 为 0
	lr2 := &LogRecord{
		Key:   []byte("tuan"),
		Value: []byte(""),
		Type:  LogRecordNormal,
	}
	res2, _ := EncodeLogRecord(lr2)
	headerBuf2 := res2[:7]

	crc2 := getLogRecordCRC(lr2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(297064154), crc2)

	lr3 := &LogRecord{
		Key:   []byte("tuan"),
		Value: []byte("tuan"),
		Type:  LogRecordDeleted,
	}
	res3, _ := EncodeLogRecord(lr3)
	headerBuf3 := res3[:7]

	crc3 := getLogRecordCRC(lr3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(292385340), crc3)
}

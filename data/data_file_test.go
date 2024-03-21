package data

import (
	"github.com/stretchr/testify/assert"
	"path"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	file1, err := OpenDataFile(path.Join("../../tem"), 0)
	assert.Nil(t, err)
	assert.NotNil(t, file1)

	file2, err := OpenDataFile(path.Join("../../tem"), 2)
	assert.Nil(t, err)
	assert.NotNil(t, file2)

	file3, err := OpenDataFile(path.Join("../../tem"), 2)
	assert.Nil(t, err)
	assert.NotNil(t, file3)

}

func TestDataFile_Write(t *testing.T) {
	file1, err := OpenDataFile(path.Join("../../tem"), 0)
	assert.Nil(t, err)
	assert.NotNil(t, file1)

	err = file1.Write([]byte("123"))
	assert.Nil(t, err)

	err = file1.Write([]byte("123"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	file1, err := OpenDataFile(path.Join("../../tem"), 0)
	assert.Nil(t, err)
	assert.NotNil(t, file1)

	err = file1.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	file1, err := OpenDataFile(path.Join("../../tem"), 123)
	assert.Nil(t, err)
	assert.NotNil(t, file1)

	err = file1.Write([]byte("123"))
	assert.Nil(t, err)

	err = file1.Sync()
	assert.Nil(t, err)
}

func TestDataFile_GetLogRecord(t *testing.T) {
	datafile, err := OpenDataFile(path.Join("../../tem"), 888)
	assert.Nil(t, err)
	assert.NotNil(t, datafile)

	rec1 := &LogRecord{
		Key:   []byte("123"),
		Value: []byte("111"),
		Type:  LogRecordNormal,
	}
	res1, size1 := EncodeLogRecord(rec1)
	err = datafile.Write(res1)
	assert.Nil(t, err)
	lr1, readSize1, err := datafile.GetLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, rec1, lr1)
	assert.Equal(t, readSize1, size1)

	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("tuan"),
		Type:  LogRecordNormal,
	}
	res2, size2 := EncodeLogRecord(rec2)
	err = datafile.Write(res2)
	assert.Nil(t, err)
	lr2, readSize2, err := datafile.GetLogRecord(size1)
	assert.Nil(t, err)
	assert.Equal(t, rec2, lr2)
	assert.Equal(t, size2, readSize2)

	rec3 := &LogRecord{
		Key:   []byte("name1"),
		Value: []byte(""),
		Type:  LogRecordDeleted,
	}
	res3, size3 := EncodeLogRecord(rec3)
	t.Log(res3)
	err = datafile.Write(res3)
	assert.Nil(t, err)
	lr3, readSize3, err := datafile.GetLogRecord(size1 + size2)
	t.Log(lr3)
	assert.Nil(t, err)
	assert.Equal(t, rec3, lr3)
	assert.Equal(t, size3, readSize3)
}

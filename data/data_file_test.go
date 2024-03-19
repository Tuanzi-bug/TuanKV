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

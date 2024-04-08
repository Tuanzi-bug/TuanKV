package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}
func TestNewFileIOManager(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}
func TestFileIO_Write(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)
	n, err = fio.Write([]byte("123"))
	assert.Equal(t, 3, n)
	assert.Nil(t, err)
	n, err = fio.Write([]byte("4"))
	assert.Equal(t, 1, n)
	assert.Nil(t, err)
	n, err = fio.Write([]byte("56"))
	assert.Equal(t, 2, n)
	assert.Nil(t, err)
}
func TestFileIO_Read(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	//n, err := fio.Write([]byte(""))
	//assert.Equal(t, 0, n)
	//assert.Nil(t, err)
	//n, err = fio.Write([]byte("123"))
	//assert.Equal(t, 3, n)
	//assert.Nil(t, err)
	//n, err = fio.Write([]byte("4"))
	//assert.Equal(t, 1, n)
	//assert.Nil(t, err)
	//n, err = fio.Write([]byte("56"))
	//assert.Equal(t, 2, n)
	//assert.Nil(t, err)

	b := make([]byte, 6)
	n, err := fio.Read(b, 0)
	assert.Equal(t, "123456", string(b))
	assert.Equal(t, 6, n)
	assert.Nil(t, err)

	b2 := make([]byte, 3)
	n, err = fio.Read(b2, 2)
	assert.Equal(t, "345", string(b2))
	assert.Equal(t, 3, n)
	assert.Nil(t, err)
}

func TestFileIO_Sync(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	fio, err := NewFileIOManager(filepath.Join("a.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}

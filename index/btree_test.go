package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBtree_Put(t *testing.T) {
	bt := NewBtree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Put([]byte("world"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
}

func TestBtree_Get(t *testing.T) {
	bt := NewBtree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Put([]byte("hello world"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	pos2 := bt.Get([]byte("hello world"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(2), pos2.Offset)
}

func TestBtree_Delete(t *testing.T) {
	bt := NewBtree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)
	res3 := bt.delete(nil)
	assert.True(t, res3)

	res2 := bt.Put([]byte("hello world"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
	res4 := bt.delete([]byte("hello world"))
	assert.True(t, res4)
}

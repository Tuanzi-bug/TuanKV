package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer is an interface that represents the index of the data records.
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool // Put is a interface method that puts the data record into the index.
	Get(key []byte) *data.LogRecordPos           // Get is a interface method that gets the data record from the index.
	Delete(key []byte) bool                      // delete is a interface method that deletes the data record from the index.
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

type IndexType = int8

const (
	Btree IndexType = iota + 1
)

func NewIndexer(indextype IndexType) Indexer {
	switch indextype {
	case Btree:
		return NewBTree()
	default:
		panic("unsupported index type")
	}
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

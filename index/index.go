package index

import (
	"bytes"
	"github.com/Tuanzi-bug/TuanKV/data"
	"github.com/google/btree"
)

// Indexer is an interface that represents the index of the data records.
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos // Put is an interface method that puts the data record into the index.
	Get(key []byte) *data.LogRecordPos                         // Get is an interface method that gets the data record from the index.
	Delete(key []byte) (*data.LogRecordPos, bool)              // delete is an interface method that deletes the data record from the index.
	Iterator(reverse bool) Iterator                            // Iterator is an interface method that returns an iterator for the index.
	Size() int                                                 // Size is an interface method that returns the size of the index.
	Close() error                                              // Close is an interface method that closes the index.
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

type IndexType = int8

const (
	Btree IndexType = iota + 1
	Art
	BPTree
)

// NewIndexer 根据配置返回对应的索引对象
func NewIndexer(indextype IndexType, dirPath string) Indexer {
	switch indextype {
	case Btree:
		return NewBTree()
	case Art:
		return NewART()
	//case BPTree:
	//	return NewBPlusTree(dirPath)
	default:
		panic("unsupported index type")
	}
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

type Iterator interface {
	Rewind()                   // 重新回到迭代器的起点
	Seek(key []byte)           //根据传入的key从此开始遍历
	Next()                     //跳转下一个key
	Valid() bool               // 是否已经遍历完所有key
	Key() []byte               // 当前遍历位置的key数据
	Value() *data.LogRecordPos // 当前遍历位置的Value数据
	Close()                    // 关闭迭代器，释放相应资源
}

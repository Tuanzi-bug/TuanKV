package index

import (
	"bitcask-go/data"
	"github.com/google/btree"
	"sync"
)

// BTree is a struct that represents the index of the data records.
// package btree google library for better use
// api: https://pkg.go.dev/github.com/google/btree
// introduction: https://www.cnblogs.com/lianzhilei/p/11250589.html
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

// NewBTree is a function that creates a new BTree instance.
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	bt.tree.ReplaceOrInsert(it)
	return true
}
func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}
func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldItem := bt.tree.Delete(it)
	if oldItem == nil {
		return false
	}
	return true
}

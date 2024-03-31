package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
	"sort"
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

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Close() error {
	return nil
}

type btreeIterator struct {
	curIndex int     //当前遍历的下标位置
	reverse  bool    //是否反向
	values   []*Item // 所有key+位置索引信息
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())
	SearchItem := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(SearchItem)
	} else {
		tree.Ascend(SearchItem)
	}
	return &btreeIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

func (bpt *btreeIterator) Rewind() {
	bpt.curIndex = 0
}
func (bpt *btreeIterator) Seek(key []byte) {
	if bpt.reverse {
		bpt.curIndex = sort.Search(len(bpt.values), func(i int) bool {
			return bytes.Compare(bpt.values[i].key, key) <= 0
		})
	} else {
		bpt.curIndex = sort.Search(len(bpt.values), func(i int) bool {
			return bytes.Compare(bpt.values[i].key, key) >= 0
		})
	}
}

func (bpt *btreeIterator) Next() {
	bpt.curIndex += 1
}
func (bpt *btreeIterator) Valid() bool {
	return bpt.curIndex < len(bpt.values)
}
func (bpt *btreeIterator) Key() []byte {
	return bpt.values[bpt.curIndex].key
}
func (bpt *btreeIterator) Value() *data.LogRecordPos {
	return bpt.values[bpt.curIndex].pos
}
func (bpt *btreeIterator) Close() {
	bpt.values = nil
}

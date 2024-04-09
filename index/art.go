package index

import (
	"bitcask-go/data"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	defer art.lock.Unlock()
	oldValue, _ := art.tree.Insert(key, pos)
	if oldValue == nil {
		return nil
	}
	return oldValue.(*data.LogRecordPos)
}
func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()

	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}
func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	defer art.lock.Unlock()

	oldValue, deleted := art.tree.Delete(key)
	if oldValue == nil {
		return nil, deleted
	}
	return oldValue.(*data.LogRecordPos), deleted
}
func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	defer art.lock.RUnlock()

	return art.tree.Size()
}
func (art *AdaptiveRadixTree) Close() error {
	return nil
}
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	if art.tree == nil {
		return nil
	}
	art.lock.RLock()
	defer art.lock.RUnlock()

	return newArtIterator(art.tree, reverse)
}

type artIterator struct {
	curIndex int     //当前遍历的下标位置
	reverse  bool    //是否反向
	values   []*Item // 所有key+位置索引信息
}

func newArtIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	values := make([]*Item, tree.Size())

	if reverse {
		idx = tree.Size() - 1
	}

	saveItems := func(node goart.Node) bool {
		values[idx] = &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveItems)
	return &artIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

func (arti *artIterator) Rewind() {
	arti.curIndex = 0
}
func (arti *artIterator) Seek(key []byte) {
	if arti.reverse {
		arti.curIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) <= 0
		})
	} else {
		arti.curIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) >= 0
		})
	}
}

func (arti *artIterator) Next() {
	arti.curIndex += 1
}
func (arti *artIterator) Valid() bool {
	return arti.curIndex < len(arti.values)
}
func (arti *artIterator) Key() []byte {
	return arti.values[arti.curIndex].key
}
func (arti *artIterator) Value() *data.LogRecordPos {
	return arti.values[arti.curIndex].pos
}
func (arti *artIterator) Close() {
	arti.values = nil
}

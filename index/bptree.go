package index

import (
	"bitcask-go/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const (
	BPlusTreeIndexFileName = "bptree-index"
)

var indexBucketName = []byte("bitcask-index")

type BPlusTree struct {
	tree *bbolt.DB
}

func NewBPlusTree(dirPath string) *BPlusTree {
	// todo: 解决bptree多个参数传递
	opts := bbolt.DefaultOptions
	opts.NoSync = true
	bpTree, err := bbolt.Open(filepath.Join(dirPath, BPlusTreeIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bpTree")
	}

	if err := bpTree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bpTree")
	}

	return &BPlusTree{tree: bpTree}
}
func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	var oldVal []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(indexBucketName)
		oldVal = b.Get(key)
		err := b.Put(key, data.EncodeLogRecordPos(pos))
		return err
	}); err != nil {
		panic("filed to put value in bpTree")
	}

	return data.DecodeLogRecordPos(oldVal)
}
func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(indexBucketName)
		v := b.Get(key)
		if len(v) != 0 {
			pos = data.DecodeLogRecordPos(v)
		}
		return nil
	}); err != nil {
		panic("failed to put value in bpTree")
	}
	return pos
}
func (bpt *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var ok bool
	var oldValue []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(indexBucketName)
		if v := b.Get(key); len(v) != 0 {
			oldValue = v
			ok = true
			return b.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value in bpTree")
	}
	return data.DecodeLogRecordPos(oldValue), ok
}

func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(indexBucketName)
		size = b.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size in bpTree")
	}
	return size
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBPlusTreeIterator(bpt.tree, reverse)
}

type bPlusTreeIterator struct {
	reverse  bool
	tx       *bbolt.Tx
	cursor   *bbolt.Cursor
	curKey   []byte
	curValue []byte
}

func newBPlusTreeIterator(tree *bbolt.DB, reverse bool) *bPlusTreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	bpi := &bPlusTreeIterator{
		reverse: reverse,
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
	}
	bpi.Rewind()
	return bpi
}

func (bpt *bPlusTreeIterator) Rewind() {
	if bpt.reverse {
		bpt.curKey, bpt.curValue = bpt.cursor.Last()
	} else {
		bpt.curKey, bpt.curValue = bpt.cursor.First()
	}
}
func (bpt *bPlusTreeIterator) Seek(key []byte) {
	bpt.curKey, bpt.curValue = bpt.cursor.Seek(key)
}

func (bpt *bPlusTreeIterator) Next() {
	if bpt.reverse {
		bpt.curKey, bpt.curValue = bpt.cursor.Prev()
	} else {
		bpt.curKey, bpt.curValue = bpt.cursor.Next()
	}

}
func (bpt *bPlusTreeIterator) Valid() bool {
	return len(bpt.curKey) != 0
}
func (bpt *bPlusTreeIterator) Key() []byte {
	return bpt.curKey
}
func (bpt *bPlusTreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpt.curValue)
}
func (bpt *bPlusTreeIterator) Close() {
	_ = bpt.tx.Rollback()
}

package bitcask_go

import (
	"bytes"
	"github.com/Tuanzi-bug/TuanKV/index"
)

type Iterator struct {
	indexIter index.Iterator
	db        *DB
	options   IteratorOptions
}

func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{
		indexIter: indexIter,
		db:        db,
		options:   opts,
	}
}

func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}
func (it *Iterator) Value() ([]byte, error) {
	pos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(pos)
}
func (it *Iterator) Close() {
	it.indexIter.Close()
}

func (it *Iterator) skipToNext() {
	PrefixLen := len(it.options.Prefix)
	if PrefixLen == 0 {
		return
	}
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if PrefixLen < len(key) && bytes.Compare(key, it.options.Prefix) == 0 {
			break
		}
	}
}

package bitcask_go

import (
	"encoding/binary"
	"github.com/Tuanzi-bug/TuanKV/data"
	"github.com/Tuanzi-bug/TuanKV/index"
	"sync"
	"sync/atomic"
)

const nonTransactionSeqNo uint64 = 0

var txnFixKey = []byte("txn-fix")

type WriteBatch struct {
	options      WriteBatchOptions
	mu           *sync.Mutex
	db           *DB
	pendingWrite map[string]*data.LogRecord
}

func (db *DB) NewWriteBatch(options WriteBatchOptions) *WriteBatch {
	if db.options.IndexType == index.BPTree && !db.seqNoFileExists && !db.isInitial {
		panic("cannot use write batch, seq no file not exists")
	}
	return &WriteBatch{
		options:      options,
		mu:           new(sync.Mutex),
		db:           db,
		pendingWrite: make(map[string]*data.LogRecord),
	}
}

func (wb *WriteBatch) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	}
	wb.pendingWrite[string(key)] = logRecord
	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	logRecordPos := wb.db.index.Get(key)

	if logRecordPos == nil {
		if _, ok := wb.pendingWrite[string(key)]; ok {
			delete(wb.pendingWrite, string(key))
		}
		return nil
	}

	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted,
	}
	wb.pendingWrite[string(key)] = logRecord
	return nil
}

func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrite) == 0 {
		return nil
	}
	if len(wb.pendingWrite) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)
	positions := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrite {
		pos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = pos
	}
	_, err := wb.db.appendLogRecord(&data.LogRecord{
		Key:   logRecordKeyWithSeq(txnFixKey, seqNo),
		Value: nil,
		Type:  data.LogRecordFinished,
	})
	if err != nil {
		return err
	}

	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}
	for _, record := range wb.pendingWrite {
		pos := positions[string(record.Key)]
		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		} else if record.Type == data.LogRecordDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}

	wb.pendingWrite = make(map[string]*data.LogRecord)
	return nil
}

func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}

func parseLogRecordKey(encKey []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(encKey)
	key := encKey[n:]
	return key, seqNo
}

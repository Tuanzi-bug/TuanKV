package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/fio"
	"bitcask-go/index"
	"bitcask-go/utils"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	seqNoKey     = "seq-no"
	fileLockName = "flock"
)

// DB is Storage engine instance of bitcask
type DB struct {
	options         Options                   // 用户的配置项
	fileIds         []uint32                  //文件对应的ID
	mu              *sync.RWMutex             //锁
	activeFile      *data.DataFile            //当前正在写入的文件
	olderFiles      map[uint32]*data.DataFile // 历史文件
	index           index.Indexer             //索引
	seqNo           uint64
	isMerging       bool
	seqNoFileExists bool
	isInitial       bool
	fileLock        *flock.Flock
	bytesWrite      uint
	reclaimSize     int64
}

type Stat struct {
	keyNum          uint
	DataFileNum     uint
	reclaimableSize int64
	DiskSize        int64
}

// Put is a method to store the key-value pair in the storage engine
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 构建一条记录
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 添加进入文件中
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	// 记录写入索引树中
	if oldValue := db.index.Put(key, pos); oldValue != nil {
		db.reclaimSize += int64(oldValue.Size)
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	// 获取key对应的文件ID以及偏置
	logRecordPos := db.index.Get(key)

	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	return db.getValueByPosition(logRecordPos)
}

func (db *DB) getValueByPosition(pos *data.LogRecordPos) ([]byte, error) {
	var dataFile *data.DataFile
	// 根据ID寻找对应文件对象
	if db.activeFile.FileId == pos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 从文件中读取内容
	value, err := dataFile.Read(pos.Offset)
	if err != nil {
		return nil, err
	}
	return value, err
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 先获取key对应的位置信息
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil
	}
	// 构建新的记录信息，状态是删除状态（对某一个数据进行删除操作，仅仅添加对应的状态记录，不进行真实删除）
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}
	// 写入记录
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(pos.Size)
	// 删除索引 -- 对用户来说该key已经删除了
	oldValue, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldValue != nil {
		db.reclaimSize += int64(oldValue.Size)
	}
	return nil
}

func Open(options Options) (*DB, error) {
	// 配置项校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool

	// 判断目录地址是否存在
	if _, err := os.Stat(options.DirPath); err != nil {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		activeFile: nil,
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType, options.DirPath),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载数据文件信息
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	if options.IndexType != index.BPTree {
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}
		// 加载索引信息（和文件信息对应）
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
		if db.options.MMapAtStartup {
			if err := db.resetIOType(); err != nil {
				return nil, err
			}
		}
	} else {
		if err := db.loadSeqNo(); err != nil {
			fmt.Println(err)
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
	}

	return db, nil
}

func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	defer iterator.Close()
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

func (db *DB) Fold(fn func(key, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		key := iterator.Key()
		if !fn(key, value) {
			break
		}
	}
	return nil
}

func (db *DB) Close() error {
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
		}
		if err := db.index.Close(); err != nil {
			panic("failed to close index")
		}
	}()
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.seqNo > 0 {
		seqDataFile, err := data.OpenSeqNoFile(db.options.DirPath)
		if err != nil {
			return err
		}
		record := &data.LogRecord{
			Key:   []byte(seqNoKey),
			Value: []byte(strconv.FormatUint(db.seqNo, 10)),
		}
		encRecord, _ := data.EncodeLogRecord(record)
		if err := seqDataFile.Write(encRecord); err != nil {
			return err
		}
		if err := seqDataFile.Sync(); err != nil {
			return err
		}
	}

	err := db.activeFile.Close()
	if err != nil {
		return err
	}
	for _, item := range db.olderFiles {
		if err = item.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	// 一条记录写入文件中，需要对该记录先进行编码操作
	encRecord, size := data.EncodeLogRecord(logRecord)

	// 特殊判断：该记录写入当前文件大于配置文件大小，需要重新生成一个新的文件。
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		//
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		db.olderFiles[db.activeFile.FileId] = db.activeFile

		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	// 当前文件的偏移值开始写
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	db.bytesWrite += uint(size)
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}

	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}
	// 返回记录所对应的文件信息
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff, Size: uint32(size)}
	return pos, nil
}

func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	// 特殊判断：如果存在正在写入文件，那么新建文件id需要在基础上+1
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	// 创建新的文件，返回相关结构体
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId, fio.StandardFIO)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

func (db *DB) loadDataFile() error {
	// 读取对应目录下的文件集
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []uint32
	// 遍历每一个文件
	for _, entry := range dirEntries {
		// 判断每个文件是否后缀是否符合要求
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// 文件id对应文件名---这里可以根据实际业务进行处理
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, uint32(fileId))
		}
	}
	// 对id进行排序，最后一个id是正在写入文件
	sort.Slice(fileIds, func(i, j int) bool {
		return fileIds[i] < fileIds[j]
	})
	db.fileIds = fileIds
	// 遍历文件id，区分历史文件id和正在写入文件id
	for i, fid := range fileIds {
		ioType := fio.StandardFIO
		if db.options.MMapAtStartup {
			ioType = fio.MemoryMap
		}
		datafile, err := data.OpenDataFile(db.options.DirPath, fid, ioType)
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			db.activeFile = datafile
		} else {
			db.olderFiles[fid] = datafile
		}
	}
	return nil
}

func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fileIds) == 0 {
		return nil
	}

	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFileName); err == nil {
		fid, err := db.getNonMergeFileId(mergeFileName)
		if err != nil {
			return err
		}
		nonMergeFileId = fid
		hasMerge = true
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		//根据类型对记录进行相对应处理
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
		}
		if oldPos == nil {
			panic(ErrIndexUpdateFailed)
		} else {
			db.reclaimSize += int64(oldPos.Size)
		}

	}
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo uint64 = nonTransactionSeqNo
	// 遍历文件id
	for i, fid := range db.fileIds {
		if hasMerge && fid < nonMergeFileId {
			continue
		}
		var dataFile *data.DataFile
		// 根据id获取对应的结构体
		if fid == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fid]
		}

		var offset int64 = 0
		// 每一个文件中记录写入索引树中
		for {
			// 获取文件中的记录信息
			logRecord, size, err := dataFile.GetLogRecord(offset)
			// 截止条件：读到文件末尾
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 获取记录对应文件信息
			logRecordPos := &data.LogRecordPos{
				Fid:    fid,
				Offset: offset,
				Size:   uint32(size),
			}

			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				if logRecord.Type == data.LogRecordFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}
			if seqNo > nonTransactionSeqNo {
				currentSeqNo = seqNo
			}
			offset += size
		}
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
		db.seqNo = currentSeqNo
	}
	return nil
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	file, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := file.GetLogRecord(0)
	if err != nil {
		return err
	}
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNoFileExists = true
	db.seqNo = seqNo
	return os.Remove(fileName)
}

func (db *DB) resetIOType() error {
	if db.activeFile == nil {
		return nil
	}
	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
		return err
	}
	for _, dataFile := range db.olderFiles {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	dataFileNum := uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFileNum += 1
	}
	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size :%v", err))
	}
	return &Stat{
		keyNum:          uint(db.index.Size()),
		DataFileNum:     dataFileNum,
		reclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}

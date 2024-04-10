package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"bitcask-go/utils"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName   = "-merge"
	mergeFinishKey = "merge.finish"
)

func (db *DB) Merge() error {
	// 判空
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	// 如果正在 merge，不能重复进行merge
	if db.isMerging {
		db.mu.Unlock() // TODO: 弄清楚这里为什么需要解锁
		return ErrMergeIsProgress
	}
	// 查询merge的数据量
	totalSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	// 判断当前无效数据量满足merge的阈值
	if float32(db.reclaimSize)/float32(totalSize) < db.options.DataFileMergeRatio {
		db.mu.Unlock()
		return ErrMergeRatioUnreached
	}
	// 判断 当前磁盘容量是否满足merge的需求，一般磁盘容量是当前数据量的两倍
	availableDiskSize, err := utils.AvailableDiskSize()
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if uint64(totalSize-db.reclaimSize) >= availableDiskSize {
		db.mu.Unlock()
		return ErrNoEnoughSpaceForMerge
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()
	// 持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	// 将活跃文件转换为旧文件
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	// 打开新的活跃文件，防止写入操作不能正常进行
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return nil
	}
	// 记录没有参加merge的id
	nonMergeFileId := db.activeFile.FileId
	// 获取需要merge的文件数据
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock() // TODO: 弄清楚这里为什么需要解锁
	// 排序
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})
	// 如果发生过merge，将其删除
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); err == nil {
		if err = os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	//创建 merge 目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	// 打开一个新的实例
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false
	defer func() {
		mergeOptions.SyncWrites = db.options.SyncWrites
	}()

	mergeDB, err := Open(mergeOptions)
	defer mergeDB.Close()
	if err != nil {
		return err
	}
	// 创建 hint 文件储存索引
	hintFile, err := data.OpenHintFile(mergePath)
	defer hintFile.Close()
	if err != nil {
		return err
	}
	// 遍历需要merge的文件
	for _, mergeFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := mergeFile.GetLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			// todo: 弄清楚对于事务完成记录是否进行清除
			if logRecordPos != nil &&
				logRecordPos.Fid == mergeFile.FileId &&
				logRecordPos.Offset == offset {
				// 清除事务标记
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecordWithLock(logRecord)
				if err != nil {
					return err
				}
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			offset += size
		}
	}
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	defer mergeFinishedFile.Close()
	if err != nil {
		return err
	}
	MergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
		Type:  0,
	}
	encRecord, _ := data.EncodeLogRecord(MergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}
	return nil
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return path.Join(dir, base+mergeDirName)
}

func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	// 判断是否存储merge目录
	if _, err := os.Stat(mergePath); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()
	// 读取merge目录文件
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		if entry.Name() == data.SeqNoFileName || entry.Name() == fileLockName {
			continue
		}
		if db.options.IndexType == index.BPTree && (entry.Name() == index.BPlusTreeIndexFileName) {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}
	if !mergeFinished {
		return nil
	}

	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return nil
	}
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	defer mergeFinishedFile.Close()
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.GetLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil

}

func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	defer hintFile.Close()
	if err != nil {
		return err
	}
	var offset int64 = 0
	for {
		record, size, err := hintFile.GetLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		pos := data.DecodeLogRecordPos(record.Value)
		db.index.Put(record.Key, pos)
		offset += size
	}
	return nil
}

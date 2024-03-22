package data

import (
	"bitcask-go/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")
)

const (
	DataFileNameSuffix = ".data"
)

type DataFile struct {
	FileId    uint32
	WriteOff  int64
	IoManager fio.IOManager
}

func OpenDataFile(dirpath string, fileId uint32) (*DataFile, error) {
	// 获取文件路径
	fileName := path.Join(dirpath, fmt.Sprintf("%d", fileId)+DataFileNameSuffix)
	// 创建文件对应的io结构体
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

func (df *DataFile) Read(offset int64) ([]byte, error) {
	lr, _, err := df.GetLogRecord(offset)
	if err != nil {
		return nil, err
	}
	return lr.Value, nil
}

func (df *DataFile) GetLogRecord(offset int64) (*LogRecord, int64, error) {
	// 按照最大头部长度进行读取
	var headerBytes int64 = maxLogRecordHeaderSize
	fileSize, err := df.IoManager.Size()
	// 特殊情况：长度超过了文件大小，则按实际的进行读取
	if headerBytes+offset > fileSize {
		headerBytes = fileSize - offset
	}
	// 读取头部信息
	heardBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	// 对头部信息进行解码
	header, headerSize := decodeLogRecordHeader(heardBuf)
	if header == nil || (header.crc == 0 && header.keySize == 0 && header.valueSize == 0) {
		return nil, 0, io.EOF
	}
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	recordSize := headerSize + keySize + valueSize

	logRecord := &LogRecord{
		Type: header.recordType,
	}
	// 读取key和value值
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}
	// 根据解码出来的头部信息和key-value信息生成crc与记录crc进行对比
	crc := getLogRecordCRC(logRecord, heardBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}

func (df *DataFile) Write(buf []byte) error {
	size, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(size)
	return nil
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return
}

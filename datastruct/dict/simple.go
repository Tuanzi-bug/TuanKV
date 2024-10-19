package dict

import (
	"encoding/binary"
	bitcask "github.com/Tuanzi-bug/TuanKV"
	"github.com/Tuanzi-bug/TuanKV/datastruct"
	"github.com/hdt3213/godis/lib/logger"
	"time"
)

type SimpleDict struct {
	db *bitcask.DB
}

func (rds *SimpleDict) Len() int {
	//TODO implement me
	panic("implement me")
}

func MakeSimpleDict() *SimpleDict {
	db, err := bitcask.Open(bitcask.DefaultOptions)
	if err != nil {
		logger.Fatal(err)
	}
	return &SimpleDict{
		db: db,
	}
}
func (rds *SimpleDict) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = datastruct.String
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index := 1
	index += binary.PutVarint(buf[index:], expire)

	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	return rds.db.Put(key, encValue)
}

func (rds *SimpleDict) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, nil
	}
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}
	dataType := encValue[0]
	if dataType != datastruct.String {
		return nil, datastruct.ErrWrongTypeOperation
	}
	index := 1
	expire, n := binary.Varint(encValue[index:])
	index += n

	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}
	return encValue[index:], nil
}

package redis

import (
	"encoding/binary"
	"errors"
	bitcask "github.com/Tuanzi-bug/TuanKV"
	"github.com/Tuanzi-bug/TuanKV/utils"
	"time"
)

var ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

type DataType = byte

const (
	String DataType = iota + 1
	Hash
	Set
	List
	ZSet
)

type DataStructure struct {
	db *bitcask.DB
}

func NewDataStructure(option bitcask.Options) (*DataStructure, error) {
	db, err := bitcask.Open(option)
	if err != nil {
		return nil, err
	}
	return &DataStructure{db: db}, nil
}

func (rds DataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
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

func (rds DataStructure) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, nil
	}
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}
	index := 1
	expire, n := binary.Varint(encValue[index:])
	index += n

	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}
	return encValue[index:], nil
}

func (rds DataStructure) HSet(key, field, value []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	var exist bool
	hk := hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}
	encKey := hk.encode()

	exist = true
	if _, err := rds.db.Get(encKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	_ = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rds DataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}
	hk := hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}
	encKey := hk.encode()
	value, err := rds.db.Get(encKey)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (rds DataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	hk := hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}
	encKey := hk.encode()

	var exist bool
	exist = true
	if _, err = rds.db.Get(encKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}

	if exist {
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}
	return exist, nil
}

func (rds DataStructure) SAdd(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	sk := setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := sk.encode()

	var ok bool
	if _, err := rds.db.Get(encKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(encKey, nil)
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}
	return ok, nil
}

func (rds DataStructure) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	sk := setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := sk.encode()
	_, err = rds.db.Get(encKey)
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, err
	}

	if errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, nil
	}
	return true, nil
}

func (rds DataStructure) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if meta.size == 0 {
		return false, nil
	}

	sk := setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}
	encKey := sk.encode()

	_, err = rds.db.Get(encKey)
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, err
	}

	if errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, nil
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(encKey)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (rds DataStructure) LPush(key, member []byte) (uint32, error) {
	return rds.pushInner(key, member, true)
}
func (rds DataStructure) RPush(key, member []byte) (uint32, error) {
	return rds.pushInner(key, member, false)
}
func (rds DataStructure) pushInner(key, member []byte, isLeft bool) (uint32, error) {
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	lk := listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	meta.size++

	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(lk.encode(), member)
	if err = wb.Commit(); err != nil {
		return 0, err
	}
	return meta.size, nil
}

func (rds DataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds DataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

func (rds DataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}

	if meta.size == 0 {
		return nil, nil
	}
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}
	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	if err = rds.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}
	return element, nil

}

func (rds DataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
		score:   score,
	}
	var exist = true
	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, err
	}
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}
	if exist {
		if score == utils.FloatFromBytes(value) {
			return false, nil
		}
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}
	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.FloatFromBytes(value),
		}
		_ = wb.Delete(oldKey.encodeWithScore())
	}
	_ = wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	_ = wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}
func (rds DataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	// 构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(value), nil
}

func (rds DataStructure) findMetadata(key []byte, dataType DataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		return nil, err
	}
	var exist bool
	var meta *metadata
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	} else {
		exist = true
		meta = decodeMetadata(metaBuf)

		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}
	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}

	return meta, nil
}

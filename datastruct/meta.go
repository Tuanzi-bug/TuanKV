package datastruct

import (
	"encoding/binary"
	"github.com/Tuanzi-bug/TuanKV/utils"
)

const (
	maxMetadataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetadata = binary.MaxVarintLen64 * 2
	initialListMark   = binary.MaxVarintLen64 / 2
)

type metadata struct {
	dataType byte
	expire   int64
	version  int64
	size     uint32
	head     uint64
	tail     uint64
}

func (md metadata) encode() []byte {
	size := maxMetadataSize
	if md.dataType == List {
		size += extraListMetadata
	}
	buf := make([]byte, size)

	buf[0] = md.dataType
	index := 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}
	return buf[:index]
}

func decodeMetadata(buf []byte) *metadata {
	dataType := buf[0]

	index := 1
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
	size, n := binary.Varint(buf[index:])
	index += n

	var head uint64
	var tail uint64

	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, _ = binary.Uvarint(buf[index:])
	}

	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

type hashInternalKey struct {
	key     []byte
	version int64
	filed   []byte
}

func (hk hashInternalKey) encode() []byte {
	size := len(hk.key) + len(hk.filed) + binary.MaxVarintLen64
	buf := make([]byte, size)

	index := 0
	copy(buf[index:], hk.key)
	index += len(hk.key)
	index += binary.PutVarint(buf[index:], hk.version)
	copy(buf[index:], hk.filed)

	return buf
}

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
}

func (sk setInternalKey) encode() []byte {
	size := len(sk.key) + len(sk.member) + binary.MaxVarintLen64 + binary.MaxVarintLen32
	buf := make([]byte, size)

	index := 0
	copy(buf[index:], sk.key)
	index += len(sk.key)

	index += binary.PutVarint(buf[index:], sk.version)

	copy(buf[index:], sk.member)
	index += len(sk.member)

	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))

	return buf
}

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lk listInternalKey) encode() []byte {
	size := len(lk.key) + binary.MaxVarintLen64 + binary.MaxVarintLen64
	buf := make([]byte, size)

	index := 0
	copy(buf[index:], lk.key)
	index += len(lk.key)

	index += binary.PutVarint(buf[index:], lk.version)
	index += binary.PutUvarint(buf[index:], lk.index)

	return buf
}

type zsetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

func (zsk zsetInternalKey) encodeWithMember() []byte {
	size := len(zsk.key) + len(zsk.member) + binary.MaxVarintLen64
	buf := make([]byte, size)

	index := 0
	copy(buf[index:], zsk.key)
	index += len(zsk.key)

	index += binary.PutVarint(buf[index:], zsk.version)

	copy(buf[index:], zsk.member)
	return buf
}

func (zsk zsetInternalKey) encodeWithScore() []byte {
	scoreBuf := utils.Float64ToBytes(zsk.score)
	size := len(zsk.key) + len(zsk.member) + len(scoreBuf) + binary.MaxVarintLen64 + binary.MaxVarintLen64
	buf := make([]byte, size)

	index := 0
	copy(buf[index:], zsk.key)
	index += len(zsk.key)

	index += binary.PutVarint(buf[index:], zsk.version)

	copy(buf[index:], zsk.member)
	index += len(zsk.member)

	copy(buf[index:], scoreBuf)
	index += len(scoreBuf)

	binary.LittleEndian.PutUint32(buf[index:], uint32(len(zsk.member)))

	return buf
}

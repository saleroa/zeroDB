package storage

import (
	"encoding/binary"
	"hash/crc32"
	"time"

	"zeroDB/global/dberror"
)

const (
	// 4 * 4 + 8 + 8 + 2 = 34
	EntryHeaderSize = 34
)

type (
	Entry struct {
		State     uint16 //高八位:数据类型，低八位：操作类型
		Crc32     uint32 //校验值，用来比对取出后是否错误
		Timestamp uint64 //entry创建的时间
		TxId      uint64 //事务id
		Meta      *Meta
	}

	Meta struct {
		Key       []byte
		Value     []byte
		Extra     []byte // 放 hash的fiedl 等一些除了kv的内容
		KeySize   uint32
		ValueSize uint32
		ExtraSize uint32
	}
)

// 返回一个新的 entry
func newInternal(key, value, extra []byte, state uint16, timestamp uint64) *Entry {
	return &Entry{
		State:     state,
		Timestamp: timestamp,
		Meta: &Meta{
			Key:       key,
			Value:     value,
			Extra:     extra,
			KeySize:   uint32(len(key)),
			ValueSize: uint32(len(value)),
			ExtraSize: uint32(len(extra)),
		},
	}
}

// 创建新的entry
func NewEntry(key, value, extra []byte, typ, mark uint16) *Entry {
	var state uint16 = 0
	// 高八位数据类型，低八位操作类型
	state = state | (typ << 8)
	state = state | mark
	return newInternal(key, value, extra, state, uint64(time.Now().UnixNano()))
}

// 创建没有 extra 的 entry
func NewEntryNoExtra(key, value []byte, t, mark uint16) *Entry {
	return NewEntry(key, value, nil, t, mark)
}

// 创建有过期时间的entry
func NewEntryWithExpire(key, value []byte, deadline int64, t, mark uint16) *Entry {
	var state uint16 = 0
	state = state | (t << 8)
	state = state | mark

	return newInternal(key, value, nil, state, uint64(deadline))
}

// 创建有事务信息的entry
func NewEntryWithTxn(key, value, extra []byte, t, mark uint16, txId uint64) *Entry {
	e := NewEntry(key, value, extra, t, mark)
	e.TxId = txId
	return e
}

// 返回entry的大小
func (e *Entry) Size() uint32 {
	return EntryHeaderSize + e.Meta.KeySize + e.Meta.ValueSize + e.Meta.ExtraSize
}

// 将 entry 编码
func (e *Entry) Encode() ([]byte, error) {
	if e == nil || e.Meta.KeySize == 0 {
		return nil, dberror.ErrInvalidEntry
	}
	ks, vs := e.Meta.KeySize, e.Meta.ValueSize
	es := e.Meta.ExtraSize

	buf := make([]byte, e.Size())

	binary.BigEndian.PutUint32(buf[4:8], ks)
	binary.BigEndian.PutUint32(buf[8:12], vs)
	binary.BigEndian.PutUint32(buf[12:16], es)
	binary.BigEndian.PutUint16(buf[16:18], e.State)
	binary.BigEndian.PutUint64(buf[18:26], e.Timestamp)
	binary.BigEndian.PutUint64(buf[26:34], e.TxId)
	copy(buf[EntryHeaderSize:EntryHeaderSize+ks], e.Meta.Key)
	copy(buf[EntryHeaderSize+ks:EntryHeaderSize+ks+vs], e.Meta.Value)
	if es > 0 {
		copy(buf[EntryHeaderSize+ks+vs:EntryHeaderSize+ks+vs+es], e.Meta.Extra)
	}
	// 用于取出后对比校验，查看是否出错
	crc := crc32.ChecksumIEEE(e.Meta.Value)
	binary.BigEndian.PutUint32(buf[0:4], crc)

	return buf, nil
}

// 解码 entry
func Decode(buf []byte) (*Entry, error) {
	ks := binary.BigEndian.Uint32(buf[4:8])
	vs := binary.BigEndian.Uint32(buf[8:12])
	es := binary.BigEndian.Uint32(buf[12:16])
	state := binary.BigEndian.Uint16(buf[16:18])
	timestamp := binary.BigEndian.Uint64(buf[18:26])
	txId := binary.BigEndian.Uint64(buf[26:34])
	crc := binary.BigEndian.Uint32(buf[0:4])

	return &Entry{
		Meta: &Meta{
			KeySize:   ks,
			ValueSize: vs,
			ExtraSize: es,
		},
		State:     state,
		Crc32:     crc,
		Timestamp: timestamp,
		TxId:      txId,
	}, nil
}

// 从 entry 中获取该 entry 的数据类型
func (e *Entry) GetType() uint16 {
	return e.State >> 8
}

// 从 entry 中获取该 entry 的类型
func (e *Entry) GetMark() uint16 {
	return e.State & (2<<7 - 1)
}
